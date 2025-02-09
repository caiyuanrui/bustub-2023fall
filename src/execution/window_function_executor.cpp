#include <algorithm>
#include <optional>
#include <vector>

#include "binder/bound_order_by.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/window_function_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  cursor_ = std::nullopt;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!cursor_.has_value()) {
    auto child_tuple = Tuple();
    auto child_rid = RID();
    auto child_tuples = std::vector<Tuple>();
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      child_tuples.push_back(child_tuple);
    }

    auto compare_tuple = [schema = child_executor_->GetOutputSchema()](
                             const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                             const Tuple &lhs, const Tuple &rhs) {
      for (const auto &[order, expr] : order_bys) {
        auto lv = expr->Evaluate(&lhs, schema);
        auto rv = expr->Evaluate(&rhs, schema);
        if (lv.CompareLessThan(rv) == CmpBool::CmpTrue) {
          return order != OrderByType::DESC;
        }
        if (lv.CompareGreaterThan(rv) == CmpBool::CmpTrue) {
          return order == OrderByType::DESC;
        }
      }
      return false;
    };

    auto partition_columns = std::unordered_set<uint32_t>();
    for (auto &[column, _] : plan_->window_functions_) {
      partition_columns.insert(column);
    }

    for (auto &[column, fn] : plan_->window_functions_) {
      auto global_orders = std::vector<std::pair<OrderByType, AbstractExpressionRef>>();
      auto partition_orders = std::vector<std::pair<OrderByType, AbstractExpressionRef>>();

      for (const auto &partition_by : fn.partition_by_) {
        global_orders.emplace_back(OrderByType::ASC, partition_by);
        partition_orders.emplace_back(OrderByType::ASC, partition_by);
      }

      for (const auto &order_by : fn.order_by_) {
        global_orders.push_back(order_by);
      }

      std::sort(child_tuples.begin(), child_tuples.end(),
                [&](const auto &lhs, const auto &rhs) { return compare_tuple(global_orders, lhs, rhs); });

      const auto &group_bys = fn.partition_by_;
      const auto &agg_expr = fn.function_;
      const auto &fn_type = fn.type_;
      const auto &order_bys = fn.order_by_;

      auto make_aggregate_key = [&](const Tuple &tuple) {
        auto key = std::vector<Value>();
        for (const auto &column : group_bys) {
          key.emplace_back(column->Evaluate(&tuple, plan_->OutputSchema()));
        }
        return AggregateKey{key};
      };
      auto make_aggregate_value = [&](const Tuple &tuple) {
        return AggregateValue{{agg_expr->Evaluate(&tuple, plan_->OutputSchema())}};
      };
      auto compare_tuple_equals = [&](const Tuple &lhs, const Tuple &rhs) {
        for (const auto &order_key : order_bys) {
          return static_cast<bool>(order_key.second->Evaluate(&lhs, plan_->OutputSchema())
                                       .CompareEquals(order_key.second->Evaluate(&rhs, plan_->OutputSchema())));
        }
        return false;
      };
      auto generate_initial_aggregate_value = [&] {
        switch (fn_type) {
          case WindowFunctionType::CountStarAggregate:
            return std::make_tuple(AggregationType::CountStarAggregate, ValueFactory::GetIntegerValue(0));
          case WindowFunctionType::CountAggregate:
            return std::make_tuple(AggregationType::CountAggregate, ValueFactory::GetNullValueByType(TypeId::INTEGER));
          case WindowFunctionType::SumAggregate:
            return std::make_tuple(AggregationType::SumAggregate, ValueFactory::GetNullValueByType(TypeId::INTEGER));
          case WindowFunctionType::MinAggregate:
            return std::make_tuple(AggregationType::MinAggregate, ValueFactory::GetNullValueByType(TypeId::INTEGER));
          case WindowFunctionType::MaxAggregate:
            return std::make_tuple(AggregationType::MaxAggregate, ValueFactory::GetNullValueByType(TypeId::INTEGER));
          case WindowFunctionType::Rank:
            return std::make_tuple(AggregationType::CountAggregate, ValueFactory::GetNullValueByType(TypeId::INTEGER));
        }
        UNREACHABLE("Unknown window function type");
      };
      auto produce_tuple = [&](size_t i, const std::vector<Value> &values) {
        if (i < tuples_.size()) {
          tuples_[i] = Tuple(values, &plan_->OutputSchema());
        } else {
          tuples_.emplace_back(values, &plan_->OutputSchema());
        }
      };

      auto [agg_type, default_value] = generate_initial_aggregate_value();
      const auto agg_exprs = std::vector<AbstractExpressionRef>{agg_expr};
      const auto agg_types = std::vector<AggregationType>{agg_type};
      auto generate_partition_value = [&, &default_value = default_value](std::vector<Value> &values, size_t i,
                                                                          uint32_t column) {
        if (partition_columns.count(column) == 0) {
          values.emplace_back(plan_->columns_[column]->Evaluate(&child_tuples[i], child_executor_->GetOutputSchema()));
        } else if (i < tuples_.size()) {
          values.emplace_back(tuples_[i].GetValue(&plan_->OutputSchema(), column));
        } else {
          values.emplace_back(default_value);
        }
      };
      auto aggregate_partition = [&, &current_column = column](size_t lower_bound, size_t upper_bound) {
        if (fn_type == WindowFunctionType::Rank) {
          size_t global_rank = 0;
          size_t partition_rank = 0;
          for (auto i = lower_bound; i != upper_bound; ++i) {
            auto values = std::vector<Value>();
            for (auto column = 0U; column < plan_->OutputSchema().GetColumnCount(); column++) {
              if (column == current_column) {
                ++global_rank;
                if (partition_rank == 0 || !compare_tuple_equals(child_tuples[i], child_tuples[i - 1])) {
                  partition_rank = global_rank;
                }
                values.emplace_back(ValueFactory::GetIntegerValue(partition_rank));
              } else {
                generate_partition_value(values, i, column);
              }
            }

            produce_tuple(i, values);
          }

          return;
        }

        auto aht = SimpleAggregationHashTable(agg_exprs, agg_types);
        auto agg_key = make_aggregate_key(child_tuples[lower_bound]);

        if (order_bys.empty()) {
          for (auto i = lower_bound; i != upper_bound; ++i) {
            auto agg_value = make_aggregate_value(child_tuples[i]);
            aht.InsertCombine(agg_key, agg_value);
          }
        }

        for (auto i = lower_bound; i != upper_bound; ++i) {
          if (!order_bys.empty()) {
            auto agg_value = make_aggregate_value(child_tuples[i]);
            aht.InsertCombine(agg_key, agg_value);
          }

          auto values = std::vector<Value>();
          for (auto column = 0U; column < plan_->OutputSchema().GetColumnCount(); column++) {
            if (column == current_column) {
              values.emplace_back(aht.Begin().Val().aggregates_.begin()[0]);
            } else {
              generate_partition_value(values, i, column);
            }
          }

          produce_tuple(i, values);
        }
      };

      for (auto i = 0U; i < child_tuples.size();) {
        auto upper_bound = std::upper_bound(
            child_tuples.begin(), child_tuples.end(), child_tuples[i],
            [&](const auto &lhs, const auto &rhs) { return compare_tuple(partition_orders, lhs, rhs); });

        aggregate_partition(i, std::distance(child_tuples.begin(), upper_bound));
        i = std::distance(child_tuples.begin(), upper_bound);
      }
    }
    cursor_ = tuples_.cbegin();
  }

  if (*cursor_ == tuples_.cend()) {
    return false;
  }

  *tuple = **cursor_;
  *rid = tuple->GetRid();
  ++*cursor_;
  return true;
}
}  // namespace bustub
