"""
Gold 레이어 gold_product_ingredients write 모듈

silver_current의 unique 성분 × kcia_cosing_gold_ingredients(S3 CSV) → gold_product_ingredients append
조인 키: silver.product_ingredients[i] ↔ inci_csv.kor_name
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import logging
from datetime import datetime

import duckdb
import pandas as pd
from pyiceberg.expressions import AlwaysTrue

from config.settings import Iceberg, S3
from gold_pipeline.write_gold import _build_arrow

logger = logging.getLogger(__name__)

_PRODUCT_INGREDIENTS_QUERY = """
WITH all_ingredients AS (
    SELECT product_id, UNNEST(product_ingredients) AS ingredient_name
    FROM silver_arrow
    WHERE product_ingredients IS NOT NULL
),
unique_ingredients AS (
    SELECT ingredient_name, COUNT(DISTINCT product_id) AS usage_count
    FROM all_ingredients
    GROUP BY ingredient_name
)
SELECT
    u.ingredient_name,
    i.inci_name,
    i.kor_name,
    i.eng_name,
    i.cosing_functions,
    i.status,
    i.cosmetic_restriction,
    i.other_restrictions,
    u.usage_count
FROM unique_ingredients u
LEFT JOIN inci_df i ON u.ingredient_name = i.kor_name
ORDER BY u.usage_count DESC, u.ingredient_name
"""


def _get_inci_s3_path(con: duckdb.DuckDBPyConnection) -> str:
    """S3에서 최신 batch의 INCI gold CSV 경로를 반환합니다."""
    df = con.execute(f"""
        SELECT
            file,
            regexp_extract(file, 'batch=([^/]+)/', 1) AS batch_id
        FROM glob('{S3.INCI_GOLD_GLOB_PATTERN}')
        ORDER BY batch_id DESC
        LIMIT 1
    """).df()

    if df.empty:
        raise RuntimeError(
            f"S3에서 INCI gold CSV를 찾지 못했습니다.\n"
            f"패턴: {S3.INCI_GOLD_GLOB_PATTERN}"
        )

    path     = df["file"].iloc[0]
    batch_id = df["batch_id"].iloc[0]
    logger.info(f"INCI gold 최신 batch: {batch_id} ({path})")
    return path


def write_gold_product_ingredients(
    catalog,
    batch_job: str,
    batch_date: datetime,
) -> None:
    """
    silver_current의 unique 성분을 INCI gold CSV와 조인하여
    gold_product_ingredients 에 append 합니다.

    추후 INCI 소스를 Iceberg 테이블로 전환할 때는 _get_inci_s3_path 대신
    catalog.load_table() 방식으로 _load_inci_data 를 교체합니다.

    Args:
        catalog   : pyiceberg Catalog 인스턴스
        batch_job : 배치 식별자 (예: "20260501_120000")
        batch_date: 배치 기준 시각 (UTC datetime)
    """
    logger.info("silver_current 로드 중...")
    silver_table = catalog.load_table(Iceberg.SILVER_CURRENT_TABLE)
    silver_arrow = silver_table.scan(
        selected_fields=("product_id", "product_ingredients")
    ).to_arrow()

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws;   LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    con.execute(f"SET s3_region='{S3.REGION}';")
    con.register("silver_arrow", silver_arrow)

    inci_path = _get_inci_s3_path(con)
    inci_df   = con.execute(f"SELECT * FROM read_csv_auto('{inci_path}')").df()
    con.register("inci_df", inci_df)

    result_df: pd.DataFrame = con.execute(_PRODUCT_INGREDIENTS_QUERY).df()
    con.close()

    total       = len(result_df)
    matched     = result_df["inci_name"].notna().sum()
    match_rate  = matched / total if total else 0
    logger.info(f"unique 성분: {total}건 | INCI 매핑 성공: {matched}건 ({match_rate:.1%})")

    result_df["batch_job"]  = batch_job
    result_df["batch_date"] = pd.to_datetime(batch_date, utc=True)

    # LEFT JOIN 미매핑 행의 NaN → None 변환 (PyArrow StringType은 float NaN 거부)
    _inci_str_cols = ["inci_name", "kor_name", "eng_name", "cosing_functions",
                      "status", "cosmetic_restriction", "other_restrictions"]
    result_df[_inci_str_cols] = result_df[_inci_str_cols].where(
        result_df[_inci_str_cols].notna(), other=None
    )

    gold_table  = catalog.load_table(Iceberg.GOLD_PRODUCT_INGREDIENTS_TABLE)
    arrow_table = _build_arrow(result_df, gold_table)
    gold_table.overwrite(arrow_table, overwrite_filter=AlwaysTrue())

    logger.info(f"gold_product_ingredients overwrite 완료: {total}건")
