//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "common/exception.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      iter_(plan_, left_child_.get(), ht_.get()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  ht_->Clear();

  Tuple tuple;
  RID rid;

  /** Construct lookup table based on right tuples */
  right_child_->Init();

  while (right_child_->Next(&tuple, &rid)) {
    auto key = GetRightJoinKey(tuple, plan_->OutputSchema(), plan_->RightJoinKeyExpressions());
    // auto key = plan_->GetRightJoinKey(tuple);
    ht_->Insert(key, tuple);
  }

  /** left exec gets initialized in iter_ */
  iter_.Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool {
  switch (plan_->GetJoinType()) {
    case JoinType::INNER:
      return InnerNext(tuple, rid);
    case JoinType::LEFT:
      return LeftNext(tuple, rid);
    default:
      throw NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
  }

  return false;
}

auto HashJoinExecutor::InnerNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool {
  while (!iter_.End()) {
    const auto [probe, right] = iter_.Current();
    iter_.Advance();

    if (right.has_value()) {
      std::vector<Value> values;

      for (const auto &column : plan_->GetLeftPlan()->OutputSchema().GetColumns()) {
        values.push_back(probe.GetValue(&plan_->GetLeftPlan()->OutputSchema(),
                                        plan_->GetLeftPlan()->OutputSchema().GetColIdx(column.GetName())));
      }
      for (const auto &column : plan_->GetRightPlan()->OutputSchema().GetColumns()) {
        values.push_back(right.value().GetValue(&plan_->GetRightPlan()->OutputSchema(),
                                                plan_->GetRightPlan()->OutputSchema().GetColIdx(column.GetName())));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

auto HashJoinExecutor::LeftNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool {
  const auto generate_null_value = [](TypeId type_id) -> Value {
    switch (type_id) {
      case INVALID:
        throw NotImplementedException("INVALID's NULL value is not implemented.");
      case BOOLEAN:
        return {type_id, BUSTUB_BOOLEAN_NULL};
      case TINYINT:
        return {type_id, BUSTUB_INT8_NULL};
      case SMALLINT:
        return {type_id, BUSTUB_INT16_NULL};
      case INTEGER:
        return {type_id, BUSTUB_INT32_NULL};
      case BIGINT:
        return {type_id, BUSTUB_INT64_NULL};
      case DECIMAL:
        return {type_id, BUSTUB_DECIMAL_NULL};
      case VARCHAR:
        throw NotImplementedException("VARCHAR's NULL value is not implemented.");
        return {type_id, nullptr, 0, false};
      case TIMESTAMP:
        return {type_id, BUSTUB_TIMESTAMP_NULL};
    }
  };
  if (!iter_.End()) {
    const auto [probe, right] = iter_.Current();
    iter_.Advance();

    std::vector<Value> values;
    for (const auto &column : plan_->GetLeftPlan()->OutputSchema().GetColumns()) {
      values.push_back(probe.GetValue(&plan_->GetLeftPlan()->OutputSchema(),
                                      plan_->GetLeftPlan()->OutputSchema().GetColIdx(column.GetName())));
    }
    if (right.has_value()) {
      for (const auto &column : plan_->GetRightPlan()->OutputSchema().GetColumns()) {
        values.push_back(right.value().GetValue(&plan_->GetRightPlan()->OutputSchema(),
                                                plan_->GetRightPlan()->OutputSchema().GetColIdx(column.GetName())));
      }
    } else {
      for (const auto &column : plan_->GetRightPlan()->OutputSchema().GetColumns()) {
        values.push_back(generate_null_value(column.GetType()));
      }
    }

    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }
  return false;
}

auto HashJoinExecutor::GetOutputSchema() const -> const Schema & { return plan_->OutputSchema(); };

}  // namespace bustub
