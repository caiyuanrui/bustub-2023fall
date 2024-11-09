#!/bin/bash

set +x
set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH" && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

cd build || exit

executable="$1"

make "$executable" -j"$(nproc)" || exit
./test/"$executable"
