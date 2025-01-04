//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  this->child_executor_->Init();
  this->has_called_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->has_called_) {
    return false;
  }

  this->has_called_ = true;

  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  auto index_infos = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple old_tuple;
  RID old_rid;

  int32_t num_rows_deleted = 0;

  // Iterate over child executor to fetch tuples to delete
  while (this->child_executor_->Next(&old_tuple, &old_rid)) {
    // Mark tuple for deletion
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, old_rid);

    // Update indexes
    for (auto index : index_infos) {
      auto key = old_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, old_rid, exec_ctx_->GetTransaction());
    }

    num_rows_deleted += 1;
  }

  // Return the number of rows deleted as output tuple
  *tuple = Tuple{{Value{TypeId::INTEGER, num_rows_deleted}}, &this->GetOutputSchema()};
  return true;
}

}  // namespace bustub
