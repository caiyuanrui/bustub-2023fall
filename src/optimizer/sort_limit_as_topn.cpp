#include <memory>
#include <vector>

#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "fmt/core.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Sort && optimized_plan->GetChildAt(0)->GetType() == PlanType::Limit) {
    auto sort_node = dynamic_cast<const SortPlanNode &>(*optimized_plan);
    auto limit_node = dynamic_cast<const LimitPlanNode &>(*optimized_plan->GetChildAt(0));
    return std::make_shared<const TopNPlanNode>(optimized_plan->output_schema_, limit_node.GetChildAt(0),
                                                sort_node.GetOrderBy(), limit_node.GetLimit());
  }

  if (optimized_plan->GetType() == PlanType::Limit && optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    auto limit_node = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto sort_node = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));
    return std::make_shared<const TopNPlanNode>(optimized_plan->output_schema_, sort_node.GetChildAt(0),
                                                sort_node.GetOrderBy(), limit_node.GetLimit());
  }

  return optimized_plan;
}

}  // namespace bustub
