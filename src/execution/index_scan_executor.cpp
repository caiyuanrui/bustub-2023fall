//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      htable_(dynamic_cast<HashTableIndexForTwoIntegerColumn *>(
          exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())) {
  if (htable_ == nullptr) {
    // LOG_ERROR("htable init failed!");
  }
}

void IndexScanExecutor::Init() { this->has_called_ = false; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->has_called_) {
    return false;
  }

  BUSTUB_ENSURE(tuple != nullptr && rid != nullptr, "tuple and rid shouldn't be nullptr");

  this->has_called_ = true;

  auto index_info = this->exec_ctx_->GetCatalog()->GetIndex(this->plan_->index_oid_);

  BUSTUB_ASSERT(
      index_info->key_schema_.GetColumnCount() == 1 && index_info->key_schema_.GetColumns()[0].GetType() == INTEGER,
      "BUSTUB only supports a single, unique integer column");

  std::vector<RID> result;
  index_info->index_->ScanKey(Tuple{{this->plan_->pred_key_->val_}, &index_info->key_schema_}, &result,
                              this->exec_ctx_->GetTransaction());

  BUSTUB_ASSERT(result.size() <= 1, "We will never insert duplicate rows into tables with indexes.");

  if (result.empty()) {
    // LOG_ERROR("Tuple with target index not found");
    return false;
  }

  auto [meta, maybe_tuple] =
      this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_)->table_->GetTuple(result.at(0));
  if (!meta.is_deleted_ &&
      (this->plan_->filter_predicate_ == nullptr ||
       this->plan_->filter_predicate_->Evaluate(&maybe_tuple, this->plan_->OutputSchema()).GetAs<bool>())) {
    *tuple = maybe_tuple;
    *rid = result.at(0);
  }

  return true;
}

}  // namespace bustub
