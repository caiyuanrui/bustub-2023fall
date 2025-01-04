//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
}

void UpdateExecutor::Init() {
  this->child_executor_->Init();
  this->has_called_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (this->has_called_) {
    return false;
  }

  this->has_called_ = true;

  auto indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_);

  int32_t num_cols_updated = 0;
  Tuple old_tuple;
  RID old_rid;
  while (this->child_executor_->Next(&old_tuple, &old_rid)) {
    // Insert the new tuple
    BUSTUB_ASSERT(
        this->table_info_->schema_.GetColumnCount() == this->plan_->target_expressions_.size(),
        "Planner should gaurantee the expressions yield the the values whose schema is the same as the table's");

    std::vector<Value> values;
    for (auto &expr : this->plan_->target_expressions_) {
      auto value = expr->Evaluate(&old_tuple, this->table_info_->schema_);
      values.push_back(value);
    }

    Tuple new_tuple(values, &this->table_info_->schema_);
    auto new_rid =
        this->table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple, this->exec_ctx_->GetLockManager(),
                                               this->exec_ctx_->GetTransaction(), this->table_info_->oid_);
    if (new_rid == std::nullopt) {
      throw Exception("UpdateExecutor: Failed to insert updated tuple");
    }

    // Delete the old tuple
    this->table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, old_rid);

    // Update the indexes
    for (auto index : indexes) {
      // Delete the old index entry
      auto old_key =
          old_tuple.KeyFromTuple(this->table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());

      // Insert the new index entry
      auto new_key =
          new_tuple.KeyFromTuple(this->table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key, *new_rid, exec_ctx_->GetTransaction());
    }

    num_cols_updated += 1;
  }

  *tuple = Tuple{{Value{TypeId::INTEGER, num_cols_updated}}, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
