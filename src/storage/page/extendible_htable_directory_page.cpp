//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"
#include <sys/types.h>

#include <cstdint>
#include <cstring>
#include <limits>

#include "common/config.h"
#include "common/exception.h"
// #include "pg_definitions.hpp"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::memset(local_depths_, 0, HTABLE_DIRECTORY_ARRAY_SIZE);
  std::memset(bucket_page_ids_, 1, HTABLE_DIRECTORY_ARRAY_SIZE);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t mask = ~(std::numeric_limits<uint32_t>::max() << global_depth_);
  return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  uint32_t local_depth = local_depths_[bucket_idx];

  if (local_depth == 0) {
    for (uint32_t i = 0; i < Size(); i++) {
      bucket_page_ids_[i] = bucket_page_id;
    }
  } else {
    for (uint32_t base_idx = bucket_idx & ~(std::numeric_limits<uint32_t>::max() << local_depth); base_idx < Size();
         base_idx += (1 << local_depth)) {
      bucket_page_ids_[base_idx] = bucket_page_id;
    }
  }
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return ImageIndex(bucket_idx, local_depths_[bucket_idx]);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ < max_depth_ && "global depth is out of range");

  uint32_t size = 1 << global_depth_;
  std::memcpy(bucket_page_ids_ + size, bucket_page_ids_, size * sizeof(bucket_page_ids_[0]));
  std::memcpy(local_depths_ + size, local_depths_, size * sizeof(local_depths_[0]));

  global_depth_ += 1;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  assert(global_depth_ > 0 && "global depth is out of range");

  for (uint32_t i = 0; i < Size(); i++) {
    if (global_depth_ <= local_depths_[i]) {
      throw ExceptionType::OUT_OF_RANGE;
    }
  }

  global_depth_ -= 1;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth = local_depths_[bucket_idx];
  assert(bucket_idx < (1 << local_depth) && "bucket index is out of range");
  return local_depth;
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(local_depths_[bucket_idx] < global_depth_ && "local depth is out of range");
  local_depths_[bucket_idx] += 1;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  assert(local_depths_[bucket_idx] > 0 && "local depth is out of range");
  local_depths_[bucket_idx] -= 1;
}

}  // namespace bustub
