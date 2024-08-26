#!/bin/bash

set +x; set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH"

cd build

./test/trie_test
./test/trie_noncopy_test
./test/trie_store_test
./test/trie_store_noncopy_test