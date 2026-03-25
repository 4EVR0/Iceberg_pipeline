"""
Silver → Gold ingredient_frequency 집계 파이프라인 오케스트레이션

실행:
    cd Iceberg_pipeline
    python src/silver_to_gold/ingredient_frequency/main.py
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.silver_to_gold.ingredient_frequency.aggregator import (
    load_silver_arrow,
    aggregate_ingredient_frequency,
)
from gold_pipeline.write_gold_ingredient_frequency import write_to_iceberg, write_csv_to_s3


if __name__ == "__main__":
    print("=== Silver → Gold ingredient_frequency 집계 시작 ===\n")

    # Step 1. Silver Iceberg 테이블 로드
    print("1. Silver 테이블 로드...")
    silver_arrow = load_silver_arrow()
    print(f"   로드 완료: {silver_arrow.num_rows}건\n")

    # Step 2. 성분 빈도 집계
    print("2. 성분 빈도 집계...")
    gold_df = aggregate_ingredient_frequency(silver_arrow)
    print()

    # Step 3. Iceberg write
    print("3. Iceberg write...")
    write_to_iceberg(gold_df)
    print()

    # Step 4. CSV 저장
    print("4. CSV 저장 (s3 data_csv/)...")
    write_csv_to_s3(gold_df)

    print("\n=== 완료 ===")