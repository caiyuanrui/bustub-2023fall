#!/bin/bash

set +x; set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

if [ -d build ]; then
    cd build || exit
else
    mkdir build && cd build || exit
fi

cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER= ..

# Formatting
make format
make check-clang-tidy-p0

make -j"$(nproc)"

# Testing
make trie_test trie_store_test -j"$(nproc)"
make trie_noncopy_test trie_store_noncopy_test -j"$(nproc)"
