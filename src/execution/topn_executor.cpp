#include <functional>
#include <queue>
#include <vector>

#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_([this](const Tuple &t1, const Tuple &t2) { return Compare(t1, t2); }) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto heap = std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp_)>(cmp_);

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    if (heap.size() > plan_->GetN()) {
      heap.pop();
    }
  }

  while (!heap.empty()) {
    st_.push_back(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (st_.empty()) {
    return false;
  }
  *tuple = st_.back();
  *rid = st_.back().GetRid();
  st_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return st_.size(); };

}  // namespace bustub
