//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleHashJoinHashTable {
 public:
  SimpleHashJoinHashTable() = default;
  ~SimpleHashJoinHashTable() = default;

  auto Get(const HashJoinKey &key) const -> std::optional<std::reference_wrapper<const HashJoinValue>> {
    auto it = ht_.find(key);
    if (it == ht_.end()) {
      return std::nullopt;
    } else {
      return std::cref(it->second);
    }
  }

  void Insert(const HashJoinKey &value, const Tuple &tuple) { ht_[value].tuples_.push_back(tuple); }
  auto Empty() -> bool { return ht_.empty(); }

  void Clear() { ht_.clear(); }

  class Iterator {
   public:
    explicit Iterator(std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter) : iter_{iter} {}

    auto Key() -> const HashJoinKey & { return iter_->first; }
    auto Val() -> const HashJoinValue & { return iter_->second; }

    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter_;
  };

  auto Begin() -> Iterator { return Iterator{ht_.begin()}; }
  auto End() -> Iterator { return Iterator{ht_.end()}; }

 private:
  std::unordered_map<HashJoinKey, HashJoinValue> ht_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool override;

  auto InnerNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool;
  auto LeftNext(Tuple *tuple, RID *rid [[maybe_unused]]) -> bool;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  class Iterator {
   public:
    Iterator(const HashJoinPlanNode *plan, AbstractExecutor *left, const SimpleHashJoinHashTable *ht)
        : plan_(plan), left_executor_(left), ht_(ht) {}

    auto Init() {
      end_ = false;

      left_executor_->Init();

      if (left_executor_->Next(&probe_tuple_, &probe_rid_)) {
        right_match_tuples_ = ht_->Get(plan_->GetLeftJoinKey(probe_tuple_));
        if (right_match_tuples_.has_value()) {
          right_match_iter_ = right_match_tuples_.value().get().tuples_.begin();
        }
        return;
      }

      end_ = true;
    }

    auto End() const -> bool { return end_; }

    /** Advance to the next matching tuple.
     *  @return true if probe is moved.
     */
    auto Advance() -> bool {
      auto FetchNextProbe = [&]() {
        if (!left_executor_->Next(&probe_tuple_, &probe_rid_)) {
          end_ = true;
          return;
        }

        const auto &key = plan_->GetLeftJoinKey(probe_tuple_);
        right_match_tuples_ = ht_->Get(key);

        if (right_match_tuples_.has_value()) {
          right_match_iter_ = right_match_tuples_.value().get().tuples_.begin();
        }
      };

      if (end_) {
        // LOG_DEBUG("Iter reached the end");
        return true;
      }

      if (!right_match_tuples_.has_value()) {
        // LOG_DEBUG("No matched right tuple");
        FetchNextProbe();
        return true;
      }

      right_match_iter_++;

      if (right_match_iter_ == right_match_tuples_.value().get().tuples_.end()) {
        // LOG_DEBUG("Right tuples are run out");
        FetchNextProbe();
        return true;
      }

      // LOG_DEBUG("Moving to next right tuple");
      return false;
    }

    auto Current() -> std::pair<const Tuple, std::optional<const Tuple>> {
      return {probe_tuple_, right_match_tuples_.has_value() ? std::make_optional(*right_match_iter_) : std::nullopt};
    }

   private:
    const HashJoinPlanNode *plan_;
    AbstractExecutor *left_executor_;
    const SimpleHashJoinHashTable *ht_;

    Tuple probe_tuple_;
    RID probe_rid_;

    std::optional<std::reference_wrapper<const HashJoinValue>> right_match_tuples_ = std::nullopt;
    std::vector<Tuple>::const_iterator right_match_iter_;
    bool end_ = false;
  };

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_, right_child_;
  std::unique_ptr<SimpleHashJoinHashTable> ht_ = std::make_unique<SimpleHashJoinHashTable>();

  /** Iterator used to tranverse the joined tuple. Don't skip null value */
  Iterator iter_;
};

}  // namespace bustub
