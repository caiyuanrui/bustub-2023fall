#include <algorithm>
#include <memory>
#include <vector>

#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto ExtractExpression(
    const AbstractExpressionRef &expr,
    std::vector<AbstractExpressionRef> &left_key_expressions,
    std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
    BUSTUB_ASSERT(cmp_expr->GetChildren().size() == 2,
                  "comp expr must have exactly two children");
    std::vector<ColumnValueExpression *> children;
    for (const auto &child : cmp_expr->GetChildren()) {
      auto cv = dynamic_cast<ColumnValueExpression *>(child.get());
      if (cv == nullptr) {
        return false;
      }
      children.push_back(cv);
    }

    std::sort(
        children.begin(), children.end(),
        [](const ColumnValueExpression *a, const ColumnValueExpression *b) {
          return a->GetTupleIdx() < b->GetTupleIdx();
        });

    left_key_expressions.push_back(
        children[0]->CloneWithChildren(children[0]->GetChildren()));
    right_key_expressions.push_back(
        children[1]->CloneWithChildren(children[1]->GetChildren()));

    return true;
  }

  auto logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    for (const auto &child : expr->GetChildren()) {
      if (!ExtractExpression(child, left_key_expressions,
                             right_key_expressions)) {
        return false;
      }
    }
    return true;
  }

  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan)
    -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of
  // conjunction of equi-condistions: E.g. <column expr> = <column expr> AND
  // <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // If the predicate is a logic expression, break it down and combine the
  // equi-conditions all together. Quit if any condition is connected by OR.
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2,
                  "NLJ should have exactly 2 children.");

    std::vector<AbstractExpressionRef> left_key_expressions{};
    std::vector<AbstractExpressionRef> right_key_expressions{};

    if (nlj_plan.Predicate() == nullptr ||
        !ExtractExpression(nlj_plan.Predicate(), left_key_expressions,
                           right_key_expressions)) {
      return optimized_plan;
    }

    return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_, optimized_plan->GetChildren()[0],
        optimized_plan->GetChildren()[1], left_key_expressions,
        right_key_expressions, nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
