#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load env vars from centralised .env
set -a
source "$PROJECT_ROOT/.env"
set +a

# Make repo packages importable
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"

# Execute python script
PYTHON_BIN="$PROJECT_ROOT/env/bin/python"
exec "$PYTHON_BIN" "$SCRIPT_DIR/main.py"
