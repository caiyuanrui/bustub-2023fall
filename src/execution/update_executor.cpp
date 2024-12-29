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

  if (this->table_info_ == nullptr) {
    throw bustub::Exception("InsertExecutor: Table not found");
  }
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  BUSTUB_ASSERT(this->child_executor_ != nullptr, "UpdateExecutor: Child executor is nullptr");
  this->child_executor_->Init();
  this->has_returned_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  if (this->has_returned_) {
    return false;
  }

  auto index_info = this->exec_ctx_->GetCatalog()->GetTableIndexes(this->table_info_->name_);

  Tuple old_tuple;
  RID old_rid;

  int32_t num_cols_updated = 0;
  while (this->child_executor_->Next(&old_tuple, &old_rid)) {
    // Delete the old tuple
    auto table = this->table_info_->table_.get();
    table->UpdateTupleMeta(TupleMeta{0, true}, old_rid);

    // Update the index
    for (IndexInfo *index : index_info) {
      auto key = tuple->KeyFromTuple(this->table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, RID{}, exec_ctx_->GetTransaction());
    }

    // Insert the new tuple
    std::vector<Value> values;
    for (uint32_t i = 0; i < this->table_info_->schema_.GetColumnCount(); i++) {
      values.push_back({this->plan_->target_expressions_[i]->Evaluate(&old_tuple, this->table_info_->schema_)});
    }

    Tuple new_tuple(values, &this->table_info_->schema_);
    auto new_rid = table->InsertTuple(TupleMeta{0, false}, new_tuple);
    if (!new_rid.has_value()) {
      throw Exception("UpdateExecutor: Failed to insert updated tuple");
    }

    num_cols_updated += 1;

    // Update the indexes
    for (IndexInfo *index : index_info) {
      // Delete the old index entry
      auto old_key =
          old_tuple.KeyFromTuple(this->table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());

      // Insert the new index entry
      auto new_key =
          new_tuple.KeyFromTuple(this->table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key, *new_rid, exec_ctx_->GetTransaction());
    }
  }

  *tuple = Tuple{{Value{TypeId::INTEGER, num_cols_updated}}, &GetOutputSchema()};
  this->has_returned_ = true;

  return num_cols_updated > 0;
}

}  // namespace bustub
