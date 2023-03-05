// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg; // 2KB

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  /** 这个参数的本意应该是，如果多个key－value的总长度超过了2KB，我就应该计算这些key的位图了。
   * 但是无奈发起的时机是由Flush决定，因此，并非2KB的数据就会发起一轮 Bloom Filter的计算,
   * 比如block_offset等于7KB时，filter_index为3，
   * 可能造成多轮的GenerateFilter函数调用，而除了第一轮的调用会产生位图，其它2轮相当于轮空，
   * 只是将result_的size再次放入filter_offsets_。 */
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

/** 返回filter_block的编码结果，用于写入table落盘
 * 编码结果包括：各个位图，各位图的offset，位图的总大小，
 * kFilterBaseLg信息 */
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  /** kFilterBaseLg信息有什么用？ */
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    /** 循环多次重入的话，由于第一次GenerateFilter调用之后，start_已经被清空
     * 因此，后面几次调用GenerateFilter都会走到这个逻辑中，
     * filter_offsets_中反复写入相同的result_.size() */
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  /** 将最后一个key的length记录进去，逻辑同AddKey中一样 */
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  /** 根据start_记录的length信息，将keys_中的key拆解到vector中 */
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  /** 创建bloomfilter，写入到result_中 */
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

/** content：编码后的filter_block信息，需要对齐进行解析 */
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 简单的校验逻辑 1 byte for base_lg_ and 4 for start of offset array
  /** kFilterBaseLg信息 */
  base_lg_ = contents[n - 1];
  /** last_word：总位图的偏移量 uint32_t array_offset = result_.size(); */
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  /** last_word偏移量之前是位图，之后是其offset */
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4; /** offset个数 = offsets的内存大小/4字节 */
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  /** ?? 相当于除以2KB，为啥要除以2KB？
   * 因为StartBlock中是每2KB调用一次GenerateFilter，记录一个filter_offset */
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    /** 找到对应的offset */
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      /** 根据offset信息，读取对应的filter信息 */
      Slice filter = Slice(data_ + start, limit - start);
      /** bloomfilter查询 */
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
