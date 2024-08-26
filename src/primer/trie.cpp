#include "primer/trie.h"
#include <deque>
#include <memory>
#include <string_view>
#include <utility>
// #include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (root_ == nullptr) {
    return nullptr;
  }

  std::shared_ptr<const TrieNode> node = this->root_;
  for (const auto &c : key) {
    auto it = node->children_.find(c);
    if (it == node->children_.end()) {
      return nullptr;
    }
    node = it->second;
  }

  if (!node->is_value_node_) {
    return nullptr;
  }

  auto value = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (value == nullptr) {
    return nullptr;
  }

  return value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::deque<std::shared_ptr<TrieNode>> que;
  que.push_back(root_ != nullptr ? root_->Clone() : std::make_shared<TrieNode>());

  for (const auto &c : key) {
    auto it = que.back()->children_.find(c);
    if (it == que.back()->children_.end()) {
      que.push_back(std::make_shared<TrieNode>());
    } else {
      que.push_back(it->second->Clone());
    }
  }

  auto leaf = std::make_unique<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  leaf->children_ = que.back()->children_;
  que.back() = std::move(leaf);

  auto iter = que.begin();
  for (const auto &c : key) {
    auto cur = *iter;
    auto next = *(iter + 1);
    cur->children_.insert_or_assign(c, next);
    iter++;
  }

  return Trie(std::move(que.front()));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  // 1. remove the node if it is a leaf, jump to 3.
  // 2. convert the node to TrieNode type if it is not a leaf
  // 3. you will need to purge all unnecessary nodes after removal
  //    which means you need to remove the parent node if it has no value

  if (key.empty() || root_ == nullptr) {
    return {};
  }

  auto que = std::deque<std::shared_ptr<TrieNode>>();
  que.push_back(root_->Clone());

  for (const auto &c : key) {
    auto it = que.back()->children_.find(c);
    if (it == que.back()->children_.end()) {
      return Trie(std::move(que.front()));
    }
    que.push_back(it->second->Clone());
  }

  if (!que.back()->children_.empty()) {
    que.back() = std::make_shared<TrieNode>(que.back()->children_);
    auto idx = 0;
    for (auto it = que.begin(); it + 1 != que.end(); it++) {
      (*it)->children_.insert_or_assign(key.at(idx), *(it + 1));
      idx++;
    }
  } else {
    // pop unused nodes
    que.pop_back();
    while (!que.empty() && !que.back()->is_value_node_ && que.back()->children_.size() == 1) {
      que.pop_back();
    }
    if (que.empty()) {
      return {};
    }
    auto idx = 0;
    for (auto it = que.begin(); it != que.end(); it++) {
      if (it + 1 == que.end()) {
        (*it)->children_.erase((*it)->children_.find(key.at(idx)));
      } else {
        (*it)->children_.insert_or_assign(key.at(idx), *(it + 1));
      }
      idx++;
    }
  }

  return Trie(std::move(que.front()));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
