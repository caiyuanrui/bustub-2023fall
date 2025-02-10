//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each
   * child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  // return v1 > v2: min heap
  // return v1 < v2: max heap
  auto Compare(const Tuple &t1, const Tuple &t2) const -> bool {
    for (const auto &[type, expr] : plan_->GetOrderBy()) {
      auto v1 = expr->Evaluate(&t1, GetOutputSchema());
      auto v2 = expr->Evaluate(&t2, GetOutputSchema());
      // max-heap
      if (v1.CompareLessThan(v2) == CmpBool::CmpTrue) {
        return type == OrderByType::ASC || type == OrderByType::DEFAULT;
      }
      // min-heap
      if (v1.CompareGreaterThan(v2) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return false;
  }

  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::function<bool(const Tuple &, const Tuple &)> cmp_;
  std::vector<Tuple> st_;
};
}  // namespace bustub
