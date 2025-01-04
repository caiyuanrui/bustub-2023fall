//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
// #include "common/macros.h"
#include "common/rid.h"
// #include "common/util/hash_util.h"
#include <cstdint>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include "container/disk/hash/disk_extendible_hash_table.h"
// #include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size),
      header_page_id_(INVALID_PAGE_ID) {
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw ExecutionException("Failed to initialize the header page");
  }
  header_guard.template AsMut<ExtendibleHTableHeaderPage>()->Init(header_max_depth_);

  // std::ostringstream ss;
  // ss << '[' << std::this_thread::get_id() << ']' << " header_max_depth = " << header_max_depth_
  //    << ", directory_max_depth = " << directory_max_depth_ << ", bucket_max_size = " << bucket_max_size_;
  // LOG_DEBUG("%s", ss.str().c_str());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *t) const -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = Hash(key);

  // std::ostringstream ss;
  // ss << '[' << std::this_thread::get_id() << ']' << " key = " << key << ", hash = " << hash;
  // LOG_DEBUG("%s", ss.str().c_str());

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.template As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  header_guard.Drop();

  auto direcotry = directory_guard.template As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = direcotry->HashToBucketIndex(hash);
  auto bucket_page_id = direcotry->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  directory_guard.Drop();

  auto bucket = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  auto ret = bucket->Lookup(key, value, cmp_);
  bucket_guard.Drop();

  if (ret && result != nullptr) {
    result->emplace_back(std::move(value));
  }

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = Hash(key);

  // std::ostringstream ss;
  // ss << '[' << std::this_thread::get_id() << ']' << " key = " << key << ", value = " << value << ", hash = " << hash;
  // LOG_DEBUG("%s", ss.str().c_str());

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.template AsMut<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    header_guard.Drop();
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V v;
  if (bucket->Lookup(key, v, cmp_)) {
    return false;
  }

  header_guard.Drop();

  while (bucket->IsFull()) {
    if (directory->GetMaxDepth() == directory->GetLocalDepth(bucket_idx)) {
      return false;
    }

    auto new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto new_bucket = new_bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);

    if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
      directory->IncrGlobalDepth();
      bucket_idx = directory->HashToBucketIndex(hash);
    }

    directory->IncrLocalDepth(bucket_idx);
    auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

    MigrateEntries(bucket, new_bucket, new_bucket_idx, directory->GetGlobalDepthMask());
  }

  return bucket->Insert(key, value, cmp_);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Q: Do we need to delete the empty page when we execute merging?
 * A: No. The page will be automatically removed by our replacer, and the `delete` will do nothing because the page has
 * already been pinned.
 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = Hash(key);

  // std::ostringstream ss;
  // ss << '[' << std::this_thread::get_id() << ']' << " key = " << key << ", hash = " << hash;
  // LOG_DEBUG("%s", ss.str().c_str());

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.template AsMut<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // header has been dropped, you shouldn't use it in the remaining code
  header_guard.Drop();

  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  auto res = bucket->Remove(key, cmp_);

  // didn't find the key
  if (!res) {
    return false;
  }

  // We might need to merge the bucket even though it's not empty because its image might be empty
  while (directory->GetLocalDepth(bucket_idx) != 0) {
    // After deletion, the bucket becomes empty, we need to merge it with it's image bucket.
    auto image_idx = directory->GetSplitImageIndex(bucket_idx);
    auto image_page_id = directory->GetBucketPageId(image_idx);
    auto image_guard = bpm_->FetchPageWrite(image_page_id);
    auto image = image_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // You can merge the bucket with its image only when at least one of them is empty
    if (!bucket->IsEmpty() && !image->IsEmpty()) {
      return true;
    }

    // You can merge them only if they has the same local depth.
    if (directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(image_idx)) {
      return true;
    }

    // We need to make sure the bucket refers to the empty bucket and the image refers to the nonempty bucket

    // Merge
    directory->DecrLocalDepth(bucket_idx);
    directory->DecrLocalDepth(image_idx);
    if (image->IsEmpty()) {
      directory->SetBucketPageId(image_idx, bucket_page_id);
    } else {
      directory->SetBucketPageId(bucket_idx, image_page_id);
    }

    while (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }

    // Recalculate bucket_idx because the global depth might change after shrink
    bucket_idx &= directory->GetGlobalDepthMask();
    if (!image->IsEmpty()) {
      bucket_guard = std::move(image_guard);
      bucket = image;
      bucket_page_id = image_page_id;
    }
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  uint32_t oid = 0;
  uint32_t nid = 0;

  for (size_t i = 0; i < old_bucket->Size(); i++) {
    auto [key, value] = old_bucket->EntryAt(i);
    auto hash_mask = Hash(key) & local_depth_mask;
    if (hash_mask == new_bucket_idx) {
      new_bucket->PutAt(nid, std::make_pair(key, value));
      nid++;
    } else {
      old_bucket->PutAt(oid, std::make_pair(key, value));
      oid++;
    }
  }

  old_bucket->SetSize(oid);
  new_bucket->SetSize(nid);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // Step 1: Create a new directory page
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto direcotry_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // Step 2: Register this direcotry in the header, and init it
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  auto directory = direcotry_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);

  auto bucket_idx = directory->HashToBucketIndex(hash);

  // Step 3: Insert the key-value pair into a new bucket
  return InsertToNewBucket(directory, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // Step 1: Create a new bucket page
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);

  // Step 2: Insert the key-value pair into the bucket
  bucket->PutAt(0, std::make_pair(key, value));
  bucket->SetSize(1);

  // Step 3: Register this bucket in the directory
  directory->SetBucketPageId(bucket_idx, bucket_page_id);

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
