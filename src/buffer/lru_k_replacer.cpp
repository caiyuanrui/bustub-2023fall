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
#include <memory>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t ts, frame_id_t fid) : history_{ts}, fid_(fid) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : head_(std::make_shared<LRUKNode>()), tail_(std::make_shared<LRUKNode>()), replacer_size_(num_frames), k_(k) {
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard guard(latch_);
  return EvictNoLock(frame_id);
}

auto LRUKReplacer::EvictNoLock(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    *frame_id = -1;
    return false;
  }

  for (auto it = head_->next_; it != tail_; it = it->next_) {
    if (it->is_evictable_) {
      *frame_id = it->fid_;
      curr_size_--;
      Detach(it);
      map_.erase(map_.find(it->fid_));
      return true;
    }
  }

  *frame_id = -1;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard guard(latch_);

  current_timestamp_ = GetTimeStamp();

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExceptionType::INVALID;
  }

  auto it = map_.find(frame_id);

  if (it != map_.end()) {
    if (it->second->history_.size() >= k_) {
      it->second->history_.pop_front();
    }
    it->second->history_.push_back(current_timestamp_);
    MoveBackward(it->second);
  } else {
    auto new_node = std::make_shared<LRUKNode>(current_timestamp_, frame_id);
    map_.insert({frame_id, new_node});
    new_node->prev_ = tail_->prev_;
    new_node->next_ = tail_;
    tail_->prev_->next_ = new_node;
    tail_->prev_ = new_node;
    MoveForward(new_node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard guard(latch_);

  auto it = map_.find(frame_id);

  if (it != map_.end()) {
    if (it->second->is_evictable_ && !set_evictable) {
      curr_size_--;

    } else if (!it->second->is_evictable_ && set_evictable) {
      if (curr_size_ >= replacer_size_) {
        auto frame_id = std::make_shared<frame_id_t>(-1);

        if (!Evict(frame_id.get())) {
          throw ExceptionType::UNKNOWN_TYPE;
        };

      } else {
        curr_size_++;
      }
    }

    it->second->is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard guard(latch_);

  auto it = map_.find(frame_id);

  if (it != map_.end()) {
    if (it->second->is_evictable_) {
      curr_size_--;
    }
    Detach(it->second);
    map_.erase(it);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetTimeStamp() -> size_t { return std::chrono::steady_clock::now().time_since_epoch().count(); }

auto LRUKReplacer::CMP(const std::shared_ptr<LRUKNode> &n1, const std::shared_ptr<LRUKNode> &n2) -> bool {
  auto k1 = n1->history_.size() == k_;
  auto k2 = n2->history_.size() == k_;

  if (k1 == k2) {
    return n1->history_.front() < n2->history_.front();
  }
  return k2;
}

auto LRUKReplacer::MoveForward(const std::shared_ptr<LRUKNode> &n) -> void {
  auto p = n->prev_;

  while (true) {
    if (CMP(p, n) || p == head_) {
      break;
    }
    p = p->prev_;
  }

  if (p->next_ == n) {
    return;
  }

  Detach(n);
  n->prev_ = p;
  n->next_ = p->next_;
  p->next_->prev_ = n;
  p->next_ = n;
}

auto LRUKReplacer::MoveBackward(const std::shared_ptr<LRUKNode> &n) -> void {
  auto p = n->next_;

  while (true) {
    if (CMP(n, p) || p == tail_) {
      break;
    }
    p = p->next_;
  }

  if (n->next_ == p) {
    return;
  }

  Detach(n);
  n->next_ = p;
  n->prev_ = p->prev_;
  p->prev_->next_ = n;
  p->prev_ = n;
}

auto LRUKReplacer::Detach(const std::shared_ptr<LRUKNode> &n) -> void {
  n->prev_->next_ = n->next_;
  n->next_->prev_ = n->prev_;
}

}  // namespace bustub
