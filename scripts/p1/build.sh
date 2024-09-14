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

make -j"$(nproc)"

# Testing
make lru_k_replacer_test buffer_pool_manager_test -j"$(nproc)"
