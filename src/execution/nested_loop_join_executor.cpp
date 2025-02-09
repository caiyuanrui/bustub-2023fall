//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&left_executor,
    std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      iterator_(std::make_unique<NLJIterator>(left_executor_.get(),
                                              right_executor_.get())) {
  if (!(plan_->GetJoinType() == JoinType::LEFT ||
        plan_->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(
        fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { iterator_->Init(); }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid [[maybe_unused]])
    -> bool {
  switch (plan_->GetJoinType()) {
    case JoinType::INNER:
      return InnerNext(tuple, rid);
    case JoinType::LEFT:
      return LeftNext(tuple, rid);
    default:
      throw NotImplementedException(
          fmt::format("join type {} not supported", plan_->GetJoinType()));
  }
}

auto NestedLoopJoinExecutor::InnerNext(Tuple *tuple, RID *rid [[maybe_unused]])
    -> bool {
  const auto filter_pred = plan_->Predicate();

  while (!iterator_->End()) {
    auto [left_tuple, maybe_right_tuple] = iterator_->Current();

    if (!maybe_right_tuple.has_value()) {
      return false;
    }

    auto value = filter_pred->EvaluateJoin(
        &left_tuple, plan_->GetLeftPlan()->OutputSchema(),
        &maybe_right_tuple.value(), plan_->GetRightPlan()->OutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      auto left_schema = plan_->GetLeftPlan()->OutputSchema();
      auto right_schema = plan_->GetRightPlan()->OutputSchema();

      std::vector<Value> values;
      for (const auto &column : left_schema.GetColumns()) {
        values.push_back(left_tuple.GetValue(
            &left_schema, left_schema.GetColIdx(column.GetName())));
      }
      for (const auto &column : right_schema.GetColumns()) {
        values.push_back(maybe_right_tuple.value().GetValue(
            &right_schema, right_schema.GetColIdx(column.GetName())));
      }

      *tuple = Tuple{values, &GetOutputSchema()};

      ++*iterator_;
      return true;
    }

    ++*iterator_;
  }

  return false;
}

auto NestedLoopJoinExecutor::LeftNext(Tuple *tuple, RID *rid [[maybe_unused]])
    -> bool {
  auto filter_pred = plan_->Predicate();

  while (!iterator_->End()) {
    auto [left_tuple, maybe_right_tuple] = iterator_->Current();

    std::vector<Value> values;
    auto left_schema = plan_->GetLeftPlan()->OutputSchema();
    auto right_schema = plan_->GetRightPlan()->OutputSchema();

    if (maybe_right_tuple.has_value()) {
      auto value = filter_pred->EvaluateJoin(
          &left_tuple, plan_->GetLeftPlan()->OutputSchema(),
          &maybe_right_tuple.value(), plan_->GetRightPlan()->OutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        has_match_ = true;

        for (const auto &column : left_schema.GetColumns()) {
          values.push_back(left_tuple.GetValue(
              &left_schema, left_schema.GetColIdx(column.GetName())));
        }
        for (const auto &column : right_schema.GetColumns()) {
          values.push_back(maybe_right_tuple.value().GetValue(
              &right_schema, right_schema.GetColIdx(column.GetName())));
        }

        *tuple = Tuple{values, &GetOutputSchema()};

        iterator_->Advance() && (has_match_ = false);
        return true;
      }
    }

    if (iterator_->Advance()) {
      if (!has_match_) {
        for (const auto &column : left_schema.GetColumns()) {
          values.push_back(left_tuple.GetValue(
              &left_schema, left_schema.GetColIdx(column.GetName())));
        }
        for (const auto &column : right_schema.GetColumns()) {
          values.push_back(Type::GenerateNullValue(column.GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
      has_match_ = false;
    }
  }

  return false;
}

}  // namespace bustub
