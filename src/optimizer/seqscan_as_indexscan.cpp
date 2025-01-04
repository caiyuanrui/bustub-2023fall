#include <algorithm>
#include <cstdio>
#include <memory>
#include <vector>
#include "binder/tokens.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/string_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// void PrintPredType(AbstractExpressionRef pred);

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }

    BUSTUB_ENSURE(seq_scan_plan.children_.empty(), "must have exactly zero children");
    const auto *table_info = this->catalog_.GetTable(seq_scan_plan.GetTableOid());
    const auto indices = this->catalog_.GetTableIndexes(table_info->name_);
    const auto *cmp_expr = dynamic_cast<ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    if (cmp_expr == nullptr) {
      // LOG_DEBUG("failed because of: dynamic cast\n");
      return optimized_plan;
    }
    if (cmp_expr->children_.size() != 2) {
      // LOG_DEBUG("failed because of: children size %zu\n", cmp_expr->children_.size());
      return optimized_plan;
    }
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      // LOG_DEBUG("failed because of: ComparisonType %d\n", static_cast<int>(cmp_expr->comp_type_));
      return optimized_plan;
    }

    // PrintPredType(cmp_expr->children_[0]);  // ColumnValueExpression
    // PrintPredType(cmp_expr->children_[1]);  // ConstantValueExpression

    const auto *cv_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->children_[0].get());
    if (cv_expr == nullptr) {
      // LOG_DEBUG("failed\n");
      return optimized_plan;
    }

    // LOG_DEBUG("col idx: %d", cv_expr->GetColIdx());

    for (const auto *index : indices) {
      auto key_attrs = index->index_->GetKeyAttrs();
      auto itr = std::find(key_attrs.begin(), key_attrs.end(), cv_expr->GetColIdx());
      if (itr != key_attrs.end()) {
        //   IndexScanPlanNode(SchemaRef output, table_oid_t table_oid, index_oid_t index_oid,
        //            AbstractExpressionRef filter_predicate = nullptr, ConstantValueExpression *pred_key = nullptr)
        // LOG_DEBUG("opt succeeds, filter pred children size %zu", seq_scan_plan.filter_predicate_->children_.size());
        auto ret = std::make_shared<IndexScanPlanNode>(
            seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index->index_oid_, seq_scan_plan.filter_predicate_,
            dynamic_cast<ConstantValueExpression *>(seq_scan_plan.filter_predicate_->children_[1].get()));
        // LOG_DEBUG("constructor succeeds");
        return ret;
      }
    }
  }

  // LOG_DEBUG("failed\n");
  return optimized_plan;
}

// void PrintPredType(AbstractExpressionRef pred) {
//   if (dynamic_cast<ArithmeticExpression *>(pred.get()) != nullptr) {
//     printf("ArithmeticExpression\n");
//   } else if (dynamic_cast<ConstantValueExpression *>(pred.get()) != nullptr) {
//     printf("ConstantValueExpression\n");
//   } else if (dynamic_cast<ComparisonExpression *>(pred.get()) != nullptr) {
//     printf("ComparisonExpression\n");
//   } else if (dynamic_cast<StringExpression *>(pred.get()) != nullptr) {
//     printf("StringExpression\n");
//   } else if (dynamic_cast<LogicExpression *>(pred.get()) != nullptr) {
//     printf("LogicExpression\n");
//   } else if (dynamic_cast<ColumnValueExpression *>(pred.get()) != nullptr) {
//     printf("ColumnValueExpression\n");
//   } else {
//     printf("AbstractExpression\n");
//   }
// }

}  // namespace bustub
