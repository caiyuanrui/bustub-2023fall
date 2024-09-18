//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <chrono>
#include <cstddef>
#include <limits>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid, size_t k) noexcept : k_(k), fid_(fid) {}

LRUKNode::LRUKNode(frame_id_t fid, size_t k, size_t current_timestamp) noexcept : k_(k), fid_(fid) {
  history_.push_back(current_timestamp);
}

void LRUKNode::UpdateHistory(size_t current_timestamp) {
  if (history_.size() >= k_) {
    history_.pop_front();
  }
  history_.push_back(current_timestamp);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard guard(latch_);

  if (curr_size_ <= 0) {
    *frame_id = -1;
    return false;
  }

  // frame_id, earliest timestamp, has INF already
  auto candidate = std::tuple<frame_id_t, size_t, bool>(-1, std::numeric_limits<size_t>::max(), false);

  // transverse node_store_ and find the least recently used node, store the result in candidate variable
  for (auto itr : node_store_) {
    // If node is inevictable, ignore it
    if (!itr.second.IsEvictable()) {
      continue;
    }

    // If has INF already, just compare INF nodes
    if (std::get<bool>(candidate)) {
      if (!itr.second.IsFull() && itr.second.EarliestTimestamp() < std::get<size_t>(candidate)) {
        std::get<frame_id_t>(candidate) = itr.first;
        std::get<size_t>(candidate) = itr.second.EarliestTimestamp();
      }
    } else {
      if (itr.second.IsFull()) {
        // Find the node with minimum earliest timestamp
        if (itr.second.EarliestTimestamp() < std::get<size_t>(candidate)) {
          std::get<frame_id_t>(candidate) = itr.second.GetFrameId();
          std::get<size_t>(candidate) = itr.second.EarliestTimestamp();
        }
      } else {
        // If node is INF, take it directly
        std::get<frame_id_t>(candidate) = itr.second.GetFrameId();
        std::get<size_t>(candidate) = itr.second.EarliestTimestamp();
        std::get<bool>(candidate) = true;
      }
    }
  }

  if (std::get<frame_id_t>(candidate) == -1) {
    return false;
  }

  node_store_.erase(std::get<frame_id_t>(candidate));
  curr_size_--;

  *frame_id = std::get<frame_id_t>(candidate);

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard guard(latch_);

  current_timestamp_ = GetTimeStamp();

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("LRUKReplacer: frame_id is invalid");
  }

  auto itr = node_store_.find(frame_id);

  if (itr == node_store_.end()) {
    // If frame doesn't exist, create a new frame entry
    node_store_.emplace(frame_id, LRUKNode(frame_id, k_, current_timestamp_));
  } else {
    // If frame already exists, update the node's timestamp
    itr->second.UpdateHistory(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard guard(latch_);

  auto itr = node_store_.find(frame_id);

  if (itr == node_store_.end()) {
    throw Exception("LRUKReplacer: frame_id is invalid");
  }

  if (itr->second.IsEvictable() && !set_evictable) {
    itr->second.SetEvictable(set_evictable);
    curr_size_--;
  } else if (!itr->second.IsEvictable() && set_evictable) {
    itr->second.SetEvictable(set_evictable);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard guard(latch_);

  auto itr = node_store_.find(frame_id);

  if (itr == node_store_.end()) {
    return;
  }
  if (itr->second.IsEvictable()) {
    node_store_.erase(itr);
    curr_size_--;
  } else {
    throw Exception("LRUKReplacer: frame_id is not evictable");
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetTimeStamp() -> size_t { return std::chrono::steady_clock::now().time_since_epoch().count(); }

}  // namespace bustub
