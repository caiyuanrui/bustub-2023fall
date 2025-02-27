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
#include <mutex>
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
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

TEST(PageGuardTest, BPMTest) {
  const size_t buffer_pool_size = 50;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  // One thread
  page_id_t page_id_temp;
  auto page0_guard = bpm->NewPageGuarded(&page_id_temp).UpgradeRead();
  ASSERT_EQ(1, page0_guard.GetPinCount());

  {
    auto page0_guard_copy0 = bpm->FetchPageRead(page_id_temp);

    ASSERT_EQ(2, page0_guard.GetPinCount());
    ASSERT_EQ(2, page0_guard_copy0.GetPinCount());

    auto page0_guard_copy1 = bpm->FetchPageRead(page_id_temp);

    ASSERT_EQ(3, page0_guard.GetPinCount());
    ASSERT_EQ(3, page0_guard_copy0.GetPinCount());
    ASSERT_EQ(3, page0_guard_copy1.GetPinCount());

    page0_guard_copy0.Drop();

    ASSERT_EQ(2, page0_guard.GetPinCount());
    ASSERT_EQ(2, page0_guard_copy1.GetPinCount());
  }

  ASSERT_EQ(1, page0_guard.GetPinCount());

  // Multithread
  auto write = [&bpm] {
    page_id_t page_id = INVALID_PAGE_ID;
    bool is_succeed = false;
    while (!is_succeed) {
      try {
        auto page_guard = bpm->NewPageGuarded(&page_id).UpgradeWrite();
        ASSERT_EQ(1, page_guard.GetPinCount());
        is_succeed = true;
      } catch (Exception excp) {
      }
    }
  };

  std::vector<std::thread> handlers;

  for (int i = 0; i < 100; i++) {
    handlers.emplace_back(std::thread(write));
  }

  for (int i = 0; i < 100; i++) {
    handlers[i].join();
  }

  std::mutex m;
  volatile int count = 0;
  page_id_t page_id = INVALID_PAGE_ID;
  bpm->NewPageGuarded(&page_id);
  auto read = [&bpm, &page_id, &m, &count] {
    auto page_guard = bpm->FetchPageRead(page_id);
    std::lock_guard<std::mutex> lock(m);
    count++;
  };

  std::vector<std::thread> ts;
  for (int i = 0; i < 100; i++) {
    ts.emplace_back(std::thread(read));
  }

  for (auto &t : ts) {
    t.join();
  }

  ASSERT_EQ(100, count);
}

template <typename F, typename... Args>
auto Launch(F f, Args... args) {
  std::thread t(f, args...);
  return t;
}

TEST(PageGuardTest, PinCountTest) {
  const size_t buffer_pool_size = 50;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  auto CreateNewPage = [](BufferPoolManager *bpm) {
    page_id_t page_id = INVALID_PAGE_ID;
    bpm->NewPage(&page_id);
  };

  // Create a new page with pid = 0
  auto t = Launch(CreateNewPage, bpm.get());
  t.join();

  auto FetchPage = [](BufferPoolManager *bpm, page_id_t page_id) {
    auto guard = bpm->FetchPageWrite(page_id);
    ASSERT_EQ(1, guard.GetPinCount());
  };

  int num_threads = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(Launch(FetchPage, bpm.get(), 0));
  }

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
}

}  // namespace bustub
