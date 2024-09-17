#!/bin/bash

set +x; set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER= ..

# Formatting
make format
make check-clang-tidy-p1
