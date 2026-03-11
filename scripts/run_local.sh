#!/bin/bash
# Run the component locally with the test data directory

cd "$(dirname "$0")/.." || exit 1
export KBC_DATADIR="./data"
python src/component.py
