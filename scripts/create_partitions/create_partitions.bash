#!/bin/bash

# Load env vars. Env vars are only available in runtime
set -a
source .env
set +a

# Execute python script
python3 create_partitions.py
