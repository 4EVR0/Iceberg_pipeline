#!/bin/bash
set -e

case "$1" in
  sync_reference)
    python reference_pipeline/sync_reference_data.py
    ;;
  bronze_to_silver)
    python src/bronze_to_silver/main.py
    ;;
  *)
    echo "Usage: $0 {sync_reference|bronze_to_silver}"
    exit 1
    ;;
esac
