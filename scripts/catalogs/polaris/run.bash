#!/bin/bash

# Load env vars from centralised .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$SCRIPT_DIR/../.env"
set +a

# Execute python script
PYTHON_BIN="$SCRIPT_DIR/../../env/bin/python"
$PYTHON_BIN "$SCRIPT_DIR/polaris.py"
