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
#include <list>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::evict_(frame_id_t *frame_id) -> bool {
  UpdateTimestamp();

  if (node_store_.empty()) {
    return false;
  }

  size_t max_distance = 0;
  frame_id_t candidate_frame = -1;
  bool has_inf = false;

  for (auto &pair : node_store_) {
    const auto &node = pair.second;
    const size_t fid = pair.first;

    if (!node.is_evictable_) {
      continue;
    }

    size_t k_distance = std::numeric_limits<size_t>::max();

    if (node.history_.size() >= k_) {
      k_distance = current_timestamp_ - node.history_.front();
    }

    if (k_distance == std::numeric_limits<size_t>::max()) {
      if (has_inf) {
        if (node_store_[candidate_frame].history_.front() > node.history_.front()) {
          candidate_frame = fid;
        }
      } else {
        has_inf = true;
        max_distance = k_distance;
        candidate_frame = fid;
      }
    } else if (k_distance > max_distance) {
      max_distance = k_distance;
      candidate_frame = fid;
    }
  }

  if (candidate_frame == -1) {
    return false;
  }

  *frame_id = candidate_frame;
  node_store_.erase(candidate_frame);
  curr_size_--;
  return true;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard guard(latch_);
  return evict_(frame_id);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard guard(latch_);

  UpdateTimestamp();

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExceptionType::OUT_OF_RANGE;
  }

  auto now = std::chrono::steady_clock::now();
  current_timestamp_ = now.time_since_epoch().count();

  auto itr = node_store_.find(frame_id);

  if (itr != node_store_.end()) {
    auto &node = itr->second;
    if (node.history_.size() >= node.k_) {
      node.history_.pop_front();
    }
    node.history_.push_back(current_timestamp_);
  } else {
    auto new_frame = LRUKNode{std::list<size_t>{current_timestamp_}, k_, frame_id, false};

    if (IsFull()) {
      frame_id_t *evict_frame_id = nullptr;
      if (!evict_(evict_frame_id)) {  // maybe? dead lock
        throw ExceptionType::OUT_OF_MEMORY;
      }
    }
    node_store_.emplace(frame_id, std::move(new_frame));
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard guard(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExceptionType::OUT_OF_RANGE;
  }
  auto itr = node_store_.find(frame_id);
  if (itr != node_store_.end()) {
    auto &node = itr->second;
    if (node.is_evictable_ == false && set_evictable == true) {
      curr_size_++;
      node.is_evictable_ = set_evictable;
    }
    if (node.is_evictable_ == true && set_evictable == false) {
      curr_size_--;
      node.is_evictable_ = set_evictable;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard guard(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExceptionType::OUT_OF_RANGE;
  }

  auto itr = node_store_.find(frame_id);

  if (itr == node_store_.end() || itr->second.is_evictable_ == false) {
    throw ExceptionType::INVALID;
  }

  node_store_.erase(itr);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
