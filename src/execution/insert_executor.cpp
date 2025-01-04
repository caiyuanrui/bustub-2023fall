//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include "catalog/catalog.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  this->has_called_ = false;
  this->child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->has_called_) {
    return false;
  }

  this->has_called_ = true;
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());

  int tuples_inserted = 0;

  Tuple tuple_;
  RID rid_;
  while (child_executor_->Next(&tuple_, &rid_)) {
    auto rid = table_info->table_->InsertTuple(TupleMeta{0, false}, tuple_, this->exec_ctx_->GetLockManager(),
                                               this->exec_ctx_->GetTransaction(), this->plan_->GetTableOid());
    if (rid == std::nullopt) {
      throw bustub::Exception("InsertExecutor: Page overflow");
    }

    rid_ = *rid;
    for (auto index : this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
      auto key = tuple_.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      if (!index->index_->InsertEntry(key, rid_, this->exec_ctx_->GetTransaction())) {
        throw bustub::Exception("InsertExecutor: Failed to update index");
      }
    }

    ++tuples_inserted;
  }

  *tuple = Tuple({Value(TypeId::INTEGER, tuples_inserted)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
