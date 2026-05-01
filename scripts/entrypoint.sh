#!/bin/bash
set -e

case "$1" in
  create_reference_tables)
    python reference_pipeline/create_reference_tables.py "${@:2}"
    ;;
  sync_reference)
    python reference_pipeline/sync_reference_data.py
    ;;
  bronze_to_silver)
    python src/bronze_to_silver/main.py
    ;;
  *)
    echo "Usage: $0 {create_reference_tables|sync_reference|bronze_to_silver}"
    exit 1
    ;;
esac
