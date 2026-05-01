"""
Silver → Gold 파이프라인 진입점

실행:
    cd Iceberg_pipeline
    python src/silver_to_gold/main.py
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.silver_to_gold.pipeline import run_pipeline


if __name__ == "__main__":
    run_pipeline()
