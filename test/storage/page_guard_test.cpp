//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <shared_mutex>

#include "buffer/buffer_pool_manager.h"
#include "fmt/core.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MyTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = -1;
  auto page0 = bpm->NewPageGuarded(&page_id_temp);
  auto page1 = bpm->NewPageGuarded(&page_id_temp);

  {
    // mutiple reads are allowed
    auto p1 = bpm->FetchPageRead(page_id_temp);
    auto p2 = bpm->FetchPageRead(page_id_temp);
    ASSERT_EQ(p1.GetData(), page1.GetData());
    ASSERT_EQ(p2.GetData(), page1.GetData());
    ASSERT_EQ(p1.PageId(), page1.PageId());
    ASSERT_EQ(p2.PageId(), page1.PageId());
  }

  { auto p1 = bpm->FetchPageWrite(0); }
  { auto p2 = bpm->FetchPageWrite(1); }
}

}  // namespace bustub
