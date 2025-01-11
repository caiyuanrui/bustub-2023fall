//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <utility>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class NLJIterator;
/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The nested loop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool override;

  auto InnerNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool;
  auto LeftNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  std::unique_ptr<NLJIterator> iterator_;

  /** Indicator whether tuples are matched for each left tuple */
  bool has_match_ = false;
};

class NLJIterator {
 public:
  NLJIterator(AbstractExecutor *left_executor, AbstractExecutor *right_executor)
      : left_executor_(left_executor), right_executor_(right_executor) {}

  auto operator++() -> NLJIterator & {
    right_status_ = right_executor_->Next(&right_tuple_, &right_rid_);
    if (right_status_) {
      return *this;
    }

    left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_executor_->Init();
    right_status_ = right_executor_->Next(&right_tuple_, &right_rid_);

    return *this;
  }

  /** [out] whether the right table has been tranversed */
  auto Advance() -> bool {
    right_status_ = right_executor_->Next(&right_tuple_, &right_rid_);
    if (right_status_) {
      return false;
    }

    left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_executor_->Init();
    right_status_ = right_executor_->Next(&right_tuple_, &right_rid_);
    return true;
  }

  auto Init() {
    left_executor_->Init();
    right_executor_->Init();
    left_status_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_status_ = right_executor_->Next(&right_tuple_, &right_rid_);
    return *this;
  }

  auto End() const { return !left_status_; }

  auto Current() -> std::pair<const Tuple, std::optional<const Tuple>> {
    return {left_tuple_, right_status_ ? std::make_optional(right_tuple_) : std::nullopt};
  }

 private:
  /** Not-owned reference */
  AbstractExecutor *left_executor_, *right_executor_;

  /** Current state */
  Tuple left_tuple_, right_tuple_;
  RID left_rid_, right_rid_;
  bool left_status_, right_status_;
};

}  // namespace bustub
