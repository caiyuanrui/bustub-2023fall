#!/bin/bash

set +x
set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

# if [ -d ./cmake-build-relwithdebinfo ]; then
#     rm -rf ./cmake-build-relwithdebinfo
# fi

mkdir cmake-build-relwithdebinfo
cd cmake-build-relwithdebinfo || exit
cmake --log-level=ERROR -Wno-dev .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make -s -j"$(nproc)" bpm-bench
./bin/bustub-bpm-bench --duration 5000 --latency 1
