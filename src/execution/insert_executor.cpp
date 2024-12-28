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
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(table_oid);

  if (table_info == nullptr) {
    throw bustub::Exception("InsertExecutor: Table not found");
  }
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  has_returned_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ASSERT(tuple != nullptr, "tuple is nullptr");
  BUSTUB_ASSERT(rid != nullptr, "rid is nullptr");

  if (this->has_returned_ || this->child_executor_ == nullptr) {
    return false;
  }

  auto catalog = this->exec_ctx_->GetCatalog();
  auto table_oid = this->plan_->GetTableOid();
  auto table_info = catalog->GetTable(table_oid);
  if (table_info == Catalog::NULL_TABLE_INFO) {
    throw bustub::Exception("InsertExecutor: Table not found");
  }

  auto table_name = table_info->name_;
  auto index_info = catalog->GetTableIndexes(table_name);
  auto table = table_info->table_.get();

  int tuples_inserted = 0;

  while (child_executor_->Next(tuple, rid)) {
    auto rid = table->InsertTuple(TupleMeta{0, false}, *tuple);
    if (rid == std::nullopt) {
      throw bustub::Exception("InsertExecutor: Page overflow");
    }

    for (IndexInfo *index : index_info) {
      auto key = tuple->KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
    }

    ++tuples_inserted;
  }

  this->has_returned_ = true;
  *tuple = Tuple({Value(TypeId::INTEGER, tuples_inserted)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
