#!/usr/bin/env bash

set -x
set -e

cd ./build || exit
rm -rf *
cmake -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang++ ..
