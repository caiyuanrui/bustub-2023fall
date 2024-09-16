#!/bin/bash

set +x
set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

cd build || exit

make lru_k_replacer_test -j"$(nproc)"

./test/lru_k_replacer_test
