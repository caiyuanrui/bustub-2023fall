#!/bin/bash

set +x; set +e

SCRIPT_PATH="$(dirname "$(realpath "$0")")"
ROOT_PATH="$(cd "$SCRIPT_PATH"/../.. && pwd -P)"
echo "$ROOT_PATH" && cd "$ROOT_PATH"

if [ ! -f GRADESCOPE.md ]; then
    python3 gradescope_sign.py
fi

cd build
make submit-p0

cd ..
python3 gradescope_sign.py