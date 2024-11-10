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
#include <functional>
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
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    LOG_WARN("The hash table hasn't initialized yet when calling `GetValue`");
    return false;
  }

  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.template As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Didn't find target directory");
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
  // Step 0: Check if the extensible hash table is initialized
  if (header_page_id_ == INVALID_PAGE_ID) {
    LOG_WARN("The hash table hasn't initialized yet when calling `insert`");
    return false;
  }

  // Step 1: Get hash value and directory page
  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.template AsMut<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  // If directory page doesn't exist, create it
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_idx, hash, key, value);
  }

  // Fetch directory page
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // Step 2: Get according bucket
  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    header_guard.Drop();
    return InsertToNewBucket(directory, bucket_idx, key, value);
  }

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // If the key is present
  V v;
  if (bucket->Lookup(key, v, cmp_)) {
    LOG_WARN("The key has existed so `insert` didn't succeed");
    return false;
  }

  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }

  // Step 3: insert key-value pair into the bucket
  std::function<bool()> splite_and_insert = [this, &hash, &directory, &bucket, &key, &value, &splite_and_insert] {
    auto bucket_idx = directory->HashToBucketIndex(hash);

    // ====================================== BEGIN: Split bucket ======================================
    if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
      if (directory->GetGlobalDepth() == directory->GetMaxDepth()) {
        LOG_WARN("Failed to split the bucket because the directory is already full");
        return false;
      }
      directory->IncrGlobalDepth();
      // You need to recalculate the index because of the change in the global depth
      bucket_idx = directory->HashToBucketIndex(hash);
    }

    // Notice the order when calling `IncrLocalDepth` and `GetSplitImageIndex`
    directory->IncrLocalDepth(bucket_idx);
    auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;

    {
      auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);

      if (new_bucket_page_id == INVALID_PAGE_ID) {
        LOG_WARN("Failed to create a bucket page");
        return false;
      }

      auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_bucket->Init(bucket_max_size_);

      directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

      MigrateEntries(bucket, new_bucket, new_bucket_idx, directory->GetLocalDepthMask(new_bucket_idx));
    }  // new_bucket_guard will drop at thisl line
    // ============================================== END ==============================================

    // After migiration, if the old bucket is still full, we need to resplit again
    if (bucket->IsFull()) {
      return splite_and_insert();
    } else {
      return bucket->Insert(key, value, cmp_);
    }
  };

  return splite_and_insert();
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    LOG_WARN("The hash table hasn't initialized yet when calling `insert`");
    return false;
  }

  auto hash = Hash(key);

  // The header is read-only, we only merge empty buckets
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.template As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto direcotry_page_id = header->GetDirectoryPageId(directory_idx);

  if (direcotry_page_id == INVALID_PAGE_ID) {
    LOG_WARN("Didn't find target directory");
    return false;
  }

  auto direcotry_guard = bpm_->FetchPageWrite(direcotry_page_id);
  auto directory = direcotry_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    LOG_WARN("Didn't find target bucket");
    return false;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket->Remove(key, cmp_)) {
    LOG_WARN("Didn't find target key");
    return false;
  }

  if (!bucket->IsEmpty()) {
    return true;
  }

  bucket_guard.Drop();

  std::function<void(page_id_t)> merge = [this, &hash, &directory, &merge](page_id_t bucket_page_id) {
    auto bucket_idx = directory->HashToBucketIndex(hash);
    auto image_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    auto image_bucket_page_id = directory->GetBucketPageId(image_bucket_idx);

    // You can merge them only when they have the same local depth
    if (directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(image_bucket_idx)) {
      LOG_ERROR("The empty bucket's local depth is not the same as the image's");
      return;
    }

    // There is no need to merge
    if (directory->GetLocalDepth(bucket_idx) == 0) {
      return;
    }

    directory->SetBucketPageId(bucket_idx, image_bucket_page_id);

    directory->DecrLocalDepth(bucket_idx);
    directory->DecrLocalDepth(image_bucket_idx);

    // Delete the empty page
    if (!bpm_->DeletePage(bucket_page_id)) {
      throw Exception("Failed to delete a page even though this page has aquired a writer lock");
    }

    // Shrink
    if (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }

    // Check if the image page is also empty, merge it if so
    WritePageGuard image_bucket_guard = bpm_->FetchPageWrite(image_bucket_page_id);
    auto image_bucket = image_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (image_bucket->IsEmpty()) {
      return merge(image_bucket_page_id);
    }
  };

  merge(bucket_page_id);

  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  return true;
}

/**
 * Provide the empty bucket's id, merge this with its sibling
 * @return bool indicate if this bucket can still be merged
 * true: you can call this func to merge idx
 * false: you can't merge
 */
// template <typename K, typename V, typename KC>
// auto DiskExtendibleHashTable<K, V, KC>::MergeEmptyBucket(ExtendibleHTableDirectoryPage *directory,
//                                                          uint32_t bucket_idx) -> bool {
//   auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
//   auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
//   auto bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

//   if (!bucket->IsEmpty()) {
//     return false;
//   }

//   auto image_idx = directory->GetSplitImageIndex(bucket_idx);
//   auto image_page_id = directory->GetBucketPageId(image_idx);

//   // If the sibling bucket has different depth, you can't merge them, or
//   // If current bucket has depth = 0
//   if (directory->GetLocalDepth(bucket_idx) == 0 ||
//       directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(image_idx)) {
//     return false;
//   }

//   // Put empty bucket back to the buffer pool
//   bucket_guard.Drop();

//   // Update directory
//   directory->SetBucketPageId(bucket_idx, image_page_id);
//   directory->DecrLocalDepth(bucket_idx);
//   directory->DecrLocalDepth(image_idx);

//   auto image_guard = bpm_->FetchPageRead(image_page_id);
//   auto image = image_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
//   return image->IsEmpty();
// }

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
    LOG_WARN("Failed to create a directory page");
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
  auto bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);

  if (bucket_page_id == INVALID_PAGE_ID) {
    LOG_WARN("Failed to create a bucket page");
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
