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
  head_->next = tail_;
  tail_->prev = head_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard guard(latch_);
  return evit_(frame_id);
}

auto LRUKReplacer::evit_(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    *frame_id = -1;
    return false;
  }

  for (auto it = head_->next; it != tail_; it = it->next) {
    if (it->is_evictable_) {
      *frame_id = it->fid_;
      curr_size_--;
      detach(it);
      map_.erase(map_.find(it->fid_));
      return true;
    }
  }

  *frame_id = -1;
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard guard(latch_);

  current_timestamp_ = getTimeStamp();

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw ExceptionType::INVALID;
  }

  auto it = map_.find(frame_id);

  if (it != map_.end()) {
    if (it->second->history_.size() >= k_) {
      it->second->history_.pop_front();
    }
    it->second->history_.push_back(current_timestamp_);
    move_backward(it->second);
  } else {
    auto new_node = std::make_shared<LRUKNode>(current_timestamp_, frame_id);
    map_.insert({frame_id, new_node});
    new_node->prev = tail_->prev;
    new_node->next = tail_;
    tail_->prev->next = new_node;
    tail_->prev = new_node;
    move_forward(new_node);
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
    detach(it->second);
    map_.erase(it);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::getTimeStamp() -> size_t { return std::chrono::steady_clock::now().time_since_epoch().count(); }

auto LRUKReplacer::cmp(std::shared_ptr<LRUKNode> n1, std::shared_ptr<LRUKNode> n2) -> bool {
  auto k1 = n1->history_.size() == k_;
  auto k2 = n2->history_.size() == k_;

  if (k1 == k2) {
    return n1->history_.front() < n2->history_.front();
  } else {
    return k2;
  }
}

auto LRUKReplacer::move_forward(std::shared_ptr<LRUKNode> n) -> void {
  auto p = n->prev;

  while (true) {
    if (cmp(p, n) || p == head_) {
      break;
    }
    p = p->prev;
  }

  if (p->next == n) {
    return;
  }

  detach(n);
  n->prev = p;
  n->next = p->next;
  p->next->prev = n;
  p->next = n;
}

auto LRUKReplacer::move_backward(std::shared_ptr<LRUKNode> n) -> void {
  auto p = n->next;

  while (true) {
    if (cmp(n, p) || p == tail_) {
      break;
    }
    p = p->next;
  }

  if (n->next == p) {
    return;
  }

  detach(n);
  n->next = p;
  n->prev = p->prev;
  p->prev->next = n;
  p->prev = n;
}

auto LRUKReplacer::detach(std::shared_ptr<LRUKNode> n) -> void {
  n->prev->next = n->next;
  n->next->prev = n->prev;
}

}  // namespace bustub
