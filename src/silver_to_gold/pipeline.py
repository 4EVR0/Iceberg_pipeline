"""
Silver → Gold 파이프라인 오케스트레이션 로직
"""

from datetime import datetime, timezone

from config.settings import Iceberg
from gold_pipeline.write_gold_product_ingredients import write_gold_product_ingredients


def run_pipeline():
    """Silver → Gold 파이프라인 전체를 실행합니다."""
    print("=== Silver → Gold 파이프라인 시작 ===\n")

    catalog    = Iceberg.get_catalog()
    now        = datetime.now(timezone.utc)
    batch_job  = now.strftime("%Y%m%d_%H%M%S")
    batch_date = now

    print("1. gold_product_ingredients 생성 중...")
    write_gold_product_ingredients(catalog, batch_job, batch_date)

    print("\n=== 완료 ===")
