//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <future>
#include <mutex>
#include <utility>

#include "common/config.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard lk(latch_);

  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else {
    if (!replacer_->Evict(&frame_id)) {
      page_id = nullptr;
      return nullptr;
    }

    page = &pages_[frame_id];

    if (page->IsDirty()) {
      // write old page back to disk
      auto p = disk_scheduler_->CreatePromise();
      auto f = p.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(p)});
      f.get();
    }
  }

  // you need to reset the candidate page first
  // page_table_.erase(frame_id); // OMG!! I debuged this for two days :(
  page_table_.erase(page->GetPageId());
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  // assign a new page id
  page->page_id_ = *page_id = AllocatePage();

  page_table_.emplace(*page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard lk(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  auto itr = page_table_.find(page_id);

  // If page exists
  if (itr != page_table_.end()) {
    auto frame_id = itr->second;
    auto page = &pages_[frame_id];  // page_ maybe not update even page_table_ has updated!!!

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    return page;
  }

  frame_id_t frame_id = -1;
  Page *page = nullptr;

  // get a frame and reset it
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    page = &pages_[frame_id];
    if (page->IsDirty()) {
      std::promise<bool> promise;
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), page->page_id_, std::move(promise)});
      future.get();
    }
  }

  page_table_.erase(page->page_id_);
  page->ResetMemory();

  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  // read data from disk
  std::promise<bool> promise;
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();

  page_table_.insert_or_assign(page_id, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard lk(latch_);

  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = itr->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;

  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }

  if (page->pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lk(latch_);

  auto itr = page_table_.find(page_id);

  if (itr == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = itr->second;
  Page *page = &pages_[frame_id];

  std::promise<bool> promise;
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
  future.get();

  page->is_dirty_ = false;

  // spdlog::warn("{} FlushPage(page_id_t {}) If write back in disk: {}", ss.str(), page_id, flag);

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lk(latch_);

  for (auto &[page_id, frame_id] : page_table_) {
    auto &page = pages_[page_id];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page.GetData(), page_id, std::move(promise)});
    future.get();
  }

  // auto futures = std::vector<std::future<bool>>();

  // for (auto itr : page_table_) {
  //   page_id_t page_id = itr.first;
  //   frame_id_t frame_id = itr.second;
  //   Page *page = &pages_[frame_id];

  //   std::promise<bool> promise;
  //   futures.push_back(promise.get_future());
  //   disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});

  //   page->is_dirty_ = false;
  // }

  // for (auto &future : futures) {
  //   if (!future.get()) {
  //     throw ExceptionType::UNKNOWN_TYPE;
  //   }
  // }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lk(latch_);

  auto itr = page_table_.find(page_id);

  if (itr == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = itr->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;
  }

  page_table_.erase(itr);

  replacer_->Remove(frame_id);

  free_list_.push_back(frame_id);

  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
