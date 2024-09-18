#!/bin/bash

set +x
set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH" || exit

if [ -d build ]; then
    cd build || exit
else
    mkdir build && cd build || exit
fi

cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DCMAKE_BUILD_TYPE=Debug ..
# use the following cmake command if you don't want Sanitizer
# cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER= ..

make -j"$(nproc)"
