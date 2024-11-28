//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  int num_keys = 8;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RemoveTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 1);

  ASSERT_TRUE(ht.Insert(0, 0));
  ASSERT_TRUE(ht.Insert(1, 1));
  ASSERT_TRUE(ht.Insert(3, 3));

  ht.VerifyIntegrity();

  ASSERT_TRUE(ht.Remove(0));
  ASSERT_TRUE(ht.Remove(1));
  ASSERT_TRUE(ht.Remove(3));

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, GrowShrinkTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());

  constexpr uint32_t header_max_depth = 9;
  constexpr uint32_t directory_max_depth = 9;
  constexpr uint32_t bucket_max_size = 511;

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(),
                                                      header_max_depth, directory_max_depth, bucket_max_size);

  // constexpr int num_keys = (1 << directory_max_depth) * bucket_max_size;
  constexpr int num_keys = 1024;

  for (int i = 0; i < num_keys; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
    std::vector<int> res;
    ASSERT_TRUE(ht.GetValue(i, &res));
    ASSERT_EQ(res[0], i);
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RecursiveMergeTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  constexpr uint32_t header_max_depth = 2;
  constexpr uint32_t directory_max_depth = 5;
  constexpr uint32_t bucket_max_size = 1;

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(),
                                                      header_max_depth, directory_max_depth, bucket_max_size);

  ht.Insert(4, 0);
  ht.Insert(5, 0);
  ht.Insert(6, 0);
  ht.Insert(14, 0);

  ht.PrintHT();

  ht.VerifyIntegrity();

  ht.Remove(5);
  ht.Remove(14);
  ht.Remove(4);

  ht.PrintHT();
  ht.VerifyIntegrity();
}

}  // namespace bustub
