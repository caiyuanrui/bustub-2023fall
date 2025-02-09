#include <algorithm>
#include <cassert>

#include "binder/bound_order_by.h"
#include "execution/executors/sort_executor.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  /** Read all tuples into memory */
  tuples_.clear();
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(),
            [order_bys = plan_->GetOrderBy(),
             schema = child_executor_->GetOutputSchema()](
                const Tuple &t1, const Tuple &t2) -> bool {
              for (const auto &[type, expr] : order_bys) {
                auto v1 = expr->Evaluate(&t1, schema);
                auto v2 = expr->Evaluate(&t2, schema);

                if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
                  return type != OrderByType::DESC;
                }
                if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
                  return type == OrderByType::DESC;
                }
              }
              return false;
            });

  cursor_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ < tuples_.size()) {
    *tuple = tuples_[cursor_];
    *rid = tuples_[cursor_].GetRid();
    cursor_++;
    return true;
  }
  return false;
}

}  // namespace bustub
