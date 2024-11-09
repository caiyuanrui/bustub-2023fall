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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
// #include "pg_definitions.hpp"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  assert(max_depth <= HTABLE_DIRECTORY_MAX_DEPTH);
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::memset(local_depths_, 0, HTABLE_DIRECTORY_ARRAY_SIZE);
  std::memset(bucket_page_ids_, INVALID_PAGE_ID, HTABLE_DIRECTORY_ARRAY_SIZE);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }

  for (auto idx : GetAllSplitImageIndex(bucket_idx, global_depth_, local_depths_[bucket_idx])) {
    bucket_page_ids_[idx] = bucket_page_id;
  }
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }
  auto depth = local_depths_[bucket_idx];
  return depth == 0 ? bucket_idx : bucket_idx ^ (1 << (depth - 1));
}

auto ExtendibleHTableDirectoryPage::GetAllSplitImageIndex(uint32_t bucket_idx, uint32_t global_depth,
                                                          uint8_t local_depth) -> std::vector<uint32_t> {
  auto num_images = 1 << (global_depth - local_depth);

  auto base_idx = bucket_idx & ((1 << local_depth) - 1);

  std::vector<uint32_t> images;
  for (int i = 0; i < num_images; i++) {
    auto mask = i << local_depth;
    images.push_back(base_idx ^ mask);
  }

  return images;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

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

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }

  auto local_depth = local_depths_[bucket_idx];
  return local_depth;
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }

  for (uint32_t bucket_idx_ : GetAllSplitImageIndex(bucket_idx, global_depth_, local_depths_[bucket_idx])) {
    local_depths_[bucket_idx_] = local_depth;
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  } else if (local_depths_[bucket_idx] >= global_depth_) {
    throw Exception("Local depth cannot increase");
  }

  for (uint32_t bucket_idx_ : GetAllSplitImageIndex(bucket_idx, global_depth_, local_depths_[bucket_idx])) {
    local_depths_[bucket_idx_] += 1;
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  } else if (local_depths_[bucket_idx] == 0) {
    throw Exception("Local depth cannot decrease");
  }

  for (uint32_t bucket_idx_ : GetAllSplitImageIndex(bucket_idx, global_depth_, local_depths_[bucket_idx])) {
    local_depths_[bucket_idx_] -= 1;
  }
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= Size()) {
    throw Exception("Bucket index is out of boundary");
  }

  auto local_depth = local_depths_[bucket_idx];
  return (1 << local_depth) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

}  // namespace bustub
