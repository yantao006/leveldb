// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // 当申请的内存 > 1kB时，单独直接申请所需的一整块内存，并返回
    // 尽量申请大点的空间，小于1kB的内存不单独申请，避免向系统申请过多细碎内存？
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // 当申请的内存 <= 1kB时，向系统申请4kB，并分配1kB出去
  // alloc_ptr_ 指向待分配的内存地址
  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes; // 剩余的内存留着下次，当申请的内存较小时，继续分配
  return result;
}


char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8; // align最小为8bytes
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // 当前alloc_ptr_地址对align取余
  size_t slop = (current_mod == 0 ? 0 : align - current_mod); // slop表示需要补空的bytes
  size_t needed = bytes + slop; // 实际需要分配的大小 = 申请大小 + 补空大小
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop; // 以align对齐，补空slop bytes后再进行分配
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // 现有空间不够，只能重新分配
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // 最后再校验下分配的内存地址 对align取余为0，表示地址对齐
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

// 通过system call申请block_bytes字节的内存大小
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result); // 放入blocks_数组中维护申请记录
  memory_usage_.fetch_add(block_bytes + sizeof(char*), /* sizeof(char*)代表的是result指针大小 */
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
