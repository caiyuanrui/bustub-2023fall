#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    if (bpm_ != nullptr && page_ != nullptr) {
      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    }
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  auto rpg = ReadPageGuard(bpm_, page_);
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  auto wpg = WritePageGuard(bpm_, page_);
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
  return wpg;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    if (guard_.bpm_ != nullptr) {
      guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    }

    guard_.page_->RUnlatch();

    guard_.bpm_ = nullptr;
    guard_.page_ = nullptr;
    guard_.is_dirty_ = false;
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    if (guard_.bpm_ != nullptr) {
      guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    }

    guard_.page_->WUnlatch();
  }
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
