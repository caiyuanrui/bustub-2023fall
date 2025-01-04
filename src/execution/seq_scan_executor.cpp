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
#include <memory>
#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_oid = this->plan_->GetTableOid();
  auto tableinfo = this->exec_ctx_->GetCatalog()->GetTable(table_oid);
  this->table_iter_ = std::make_unique<TableIterator>(tableinfo->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(this->table_iter_ != nullptr, "You should call Init method first before Next method gets called");

  while (!this->table_iter_->IsEnd()) {
    // BUSTUB_ASSERT(this->plan_->filter_predicate_ == nullptr, "Filter predicate in seqscan is not implementedyet!");

    auto [tuple_meta_temp, tuple_temp] = this->table_iter_->GetTuple();

    if (this->plan_->filter_predicate_ != nullptr) {
      auto value = this->plan_->filter_predicate_->Evaluate(&tuple_temp, this->plan_->OutputSchema());
      // How to use value join?
      if (!value.GetAs<bool>()) {
        ++*this->table_iter_;
        continue;
      }
    }

    if (!tuple_meta_temp.is_deleted_) {
      *tuple = tuple_temp;
      *rid = tuple_temp.GetRid();

      ++*this->table_iter_;  // Move to the next tuple
      return true;
    }

    ++*this->table_iter_;  // Ensure the iterator always advances
  }

  return false;
}

}  // namespace bustub
