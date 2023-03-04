// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// 将key-value对添加到TableBuilder中
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    // 确保添加的key必须是有序的，即本次添加的key必须大于之前的任意key
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  /******************************************************
   *  *更新index_block的部分*
   *  index_block中的内容是：key=分割key， value=上一个data_block的（offset, size）
   *  因此index_block的写入时机是当data_block插入第一个key-value的时候，需要计算分割key，
   *  以及存入index_block
   *
   *  pending_index_entry：标志位，用来判断是不是data_block的第一个key，
   *  因此在上一个data_block Flush的时候，就会将标志位置位（true），
   *  当下一个data_block第一个key-value到来后，成功往index_block插入分割key之后，就会清零。
   *****************************************************/
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    /*****************************************************
     * 计算出来的分割key即r->last_key作为key，而上一个data_block的位置信息作为value
     * pending_handle里面存放的是上一个data_block的位置信息，BlockHandler类型
     *****************************************************/
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 更新filter_block的部分
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 向data_block中添加一组key-value pair
  r->last_key.assign(key.data(), key.size()); // 更新last_key的值
  r->num_entries++; // 更新计数
  r->data_block.Add(key, value); // 具体的添加操作，使用BlockBuild接口进行添加

  // 当data_block的size超过4KB时，flush到sstable file中
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) { // options.block_size默认为4KB
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  /** 将data_block写入带file中，并将其offset和size记录到pending_handle中。
   * 即pending_handle记录的是上一个data_block的信息，用于index_block的信息记录
   * */
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true; // pending_index_entry置位
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

/** 将block写入带file中，并将block的offset和size记录到handle中
 * 注意：handle中的size不包括type和crc内容
 * */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

/** 将数据从rep_写入到文件系统中 */
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();  /** 1、写入尚未Flush的data_block */
  assert(!r->closed); // r之前没被写入到文件过
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  /*************************************************
   * Data Blocks: 存储一系列有序的key-value
   * Meta Block：存储key-value对应的filter(默认为bloom filter)
   * metaindex block: 指向Meta Block的索引
   * Index BLocks: 指向Data Blocks的索引
   * Footer : 指向索引的索引
   ************************************************/

  /** 2、写入filter_block块，即图中的meta block */
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  /** 3、写入metaindex block */
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  /** 4、写入index block */
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    /** 写入index_block的内容
     * 最重要的是，算出index_block在file中的offset和size，存放到index_block_handle中，
     * 这个信息要记录在footer中
     */
    WriteBlock(&r->index_block, &index_block_handle);
  }

  /** 5、写入footer， footer为固定长度，在文件的最尾部 */
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
