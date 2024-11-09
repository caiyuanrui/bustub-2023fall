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
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw ExecutionException("Failed to initialize the header page");
  }
  header_guard.AsMut<ExtendibleHTableHeaderPage>()->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw Exception("Haven't initialized yet");
  }

  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto direcotry = directory_guard.template As<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = direcotry->HashToBucketIndex(hash);
  auto bucket_page_id = direcotry->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  auto ret = bucket->Lookup(key, value, cmp_);

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
    throw Exception("Haven't initialized the hash table yet before calling `insert` method");
  }

  // Step 1: Get hash value and directory page
  auto hash = Hash(key);

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();

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
    LOG_WARN("the key has existed so `insert` didn't succeed");
    return false;
  }

  // Step 3: insert key-value pair into the bucket

  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }

  // The directory is already full
  if (directory->GetLocalDepth(bucket_idx) == directory->GetMaxDepth()) {
    return false;
  }

  if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
    directory->IncrGlobalDepth();
    directory->IncrLocalDepth(bucket_idx);
  } else {
    directory->IncrLocalDepth(bucket_idx);
  }

  // Try to split the bucket
  auto local_depth = directory->GetLocalDepth(bucket_idx);

  // Create a new bucket as the image bucket
  auto new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();

  if (new_bucket_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to create a bucket");
  }

  auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(bucket_max_size_);

  auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  auto local_depth_mask = directory->GetLocalDepthMask(bucket_idx);

  // Register this bucket into the directory
  directory->SetLocalDepth(new_bucket_idx, local_depth);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

  MigrateEntries(bucket, new_bucket, new_bucket_idx, local_depth_mask);

  // Try inserting again
  bucket_idx = directory->HashToBucketIndex(hash);
  if (bucket_idx == new_bucket_idx) {
    if (new_bucket->IsFull()) {
      header_guard.Drop();
      directory_guard.Drop();
      bucket_guard.Drop();
      new_bucket_guard.Drop();
      return Insert(key, value, transaction);
    }
    return new_bucket->Insert(key, value, cmp_);
  } else {
    if (bucket->IsFull()) {
      header_guard.Drop();
      directory_guard.Drop();
      bucket_guard.Drop();
      new_bucket_guard.Drop();
      return Insert(key, value, transaction);
    }
    return bucket->Insert(key, value, cmp_);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    throw Exception("Haven't initialized the hash table yet before calling `insert` method");
  }

  auto hash = Hash(key);

  // The header is read-only, we only merge empty buckets
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto direcotry_page_id = header->GetDirectoryPageId(directory_idx);

  if (direcotry_page_id == INVALID_PAGE_ID) {
    return true;
  }

  auto direcotry_guard = bpm_->FetchPageWrite(direcotry_page_id);
  auto directory = direcotry_guard.template AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return true;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  auto ret = bucket->Remove(key, cmp_);

  if (!ret) {
    return false;
  }

  if (bucket->IsEmpty()) {
    auto image_idx = directory->GetSplitImageIndex(bucket_idx);
    auto image_page_id = directory->GetBucketPageId(image_idx);

    // If the sibling bucket has different depth, you can't merge them
    // If current bucket has depth = 0
    if (directory->GetLocalDepth(bucket_idx) == 0 ||
        directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(image_idx)) {
      return true;
    }

    // Put empty bucket back to buffer pool
    bucket_guard.Drop();

    // Update directory
    directory->SetBucketPageId(bucket_idx, image_page_id);
    directory->DecrLocalDepth(bucket_idx);
    directory->DecrLocalDepth(image_idx);
  }

  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  return ret;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  uint32_t oid = 0, nid = 0;

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
    throw Exception("Failed to create a new directory");
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
    throw Exception("Failed to create a new bucket");
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

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                    ExtendibleHTableBucketPage<K, V, KC> *old_bucket) {
  assert(directory != nullptr && old_bucket != nullptr);

  auto local_depth = directory->GetLocalDepth(bucket_idx);
  auto global_depth = directory->GetGlobalDepth();

  assert(local_depth < global_depth);

  // Step 1: create a new bucket
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();

  if (new_bucket_page_id == INVALID_PAGE_ID) {
    throw Exception("Failed to create a new bucket");
  }

  auto new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket->Init(old_bucket->MaxSize());

  page_id_t new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);

  directory->IncrLocalDepth(bucket_idx);
  directory->SetLocalDepth(new_bucket_idx, local_depth + 1);

  // Step 2: redistribute keys
  uint32_t local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
  MigrateEntries(old_bucket, new_bucket, new_bucket_idx, local_depth_mask);

  // Step 3: update directory's index
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
