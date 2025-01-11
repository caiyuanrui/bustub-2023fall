//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>
#include "common/macros.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  has_value_ = false;

  if (child_executor_ == nullptr) {
    aht_iterator_ = aht_.Begin();
    return;
  }

  child_executor_->Init();

  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    has_value_ = true;
    auto key = MakeAggregateKey(&tuple);
    auto val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, val);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // If the table is empty or doesn't exist
  if (!has_value_) {
    // If group by clause exists, don't return anything
    if (!plan_->group_bys_.empty()) {
      has_value_ = true;
      return false;
    } else {
      // If group by clause doesn't exist, return an empty tuple
      *tuple = GenerateInitialTuple();
      has_value_ = true;
      return true;
    }
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  const auto &key = aht_iterator_.Key();
  const auto &val = aht_iterator_.Val();

  std::vector<Value> values;
  values.reserve(key.group_bys_.size() + val.aggregates_.size());
  values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
  values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());
  BUSTUB_ASSERT(values.size() == GetOutputSchema().GetColumnCount(),
                "the size of the tuple and the schema is inconsistent");
  *tuple = Tuple{values, &GetOutputSchema()};

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

auto AggregationExecutor::GenerateInitialTuple() -> Tuple {
  std::vector<Value> vals;

  for (const auto &agg_type : plan_->GetAggregateTypes()) {
    switch (agg_type) {
      case AggregationType::CountStarAggregate:
        vals.emplace_back(ValueFactory::GetIntegerValue(0));
        break;
      case AggregationType::CountAggregate:
      case AggregationType::SumAggregate:
      case AggregationType::MinAggregate:
      case AggregationType::MaxAggregate:
        vals.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        break;
    }
  }

  return {vals, &GetOutputSchema()};
}

}  // namespace bustub
