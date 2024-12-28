//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // invariant checks
  BUSTUB_ASSERT(this->plan_->GetType() == PlanType::SeqScan, "the plan type doesn't match the invoked function");
  BUSTUB_ASSERT(this->exec_ctx_ != nullptr, "seqscan executor context is nullptr");
  BUSTUB_ASSERT(this->plan_ != nullptr, "seqscan executor plan is nullptr");

  auto cata_log = this->exec_ctx_->GetCatalog();
  BUSTUB_ASSERT(cata_log != nullptr, "catalog is nullptr");

  auto table_oid = this->plan_->GetTableOid();

  auto table_info = cata_log->GetTable(table_oid);
  BUSTUB_ASSERT(table_info != Catalog::NULL_TABLE_INFO, "table info is nullptr");

  table_iter_.emplace(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!this->table_iter_.has_value() || this->table_iter_->IsEnd()) {
    this->table_iter_.reset();
    return false;
  }

  auto [tuple_meta_, tuple_] = this->table_iter_->GetTuple();
  auto rid_ = this->table_iter_->GetRID();

  *tuple = tuple_;
  *rid = rid_;

  ++this->table_iter_.value();

  return true;
}

}  // namespace bustub
