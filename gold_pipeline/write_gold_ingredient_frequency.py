"""
Gold ingredient_frequency 테이블 Iceberg write + CSV 저장 모듈
"""

import io
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa

from config.settings import S3, Iceberg


# ==========================================
# PyArrow 스키마
# ==========================================

# Iceberg gold_ingredient_frequency 테이블 스키마와 1:1 대응
_GOLD_INGREDIENT_FREQUENCY_PA_SCHEMA = pa.schema([
    pa.field("category_id",     pa.string(),  nullable=True),
    pa.field("ingredient_name", pa.string(),  nullable=True),
    pa.field("usage_count",     pa.int64(),   nullable=True),
    pa.field("rank",            pa.int32(),   nullable=True),
])


# ==========================================
# Arrow 변환
# ==========================================

def _to_arrow_gold(df: pd.DataFrame) -> pa.Table:
    """
    gold ingredient_frequency DataFrame을 Iceberg 스키마에 맞는 PyArrow Table로 변환합니다.
    """
    table = pa.table(
        {
            "category_id":     pa.array(df["category_id"],     type=pa.string()),
            "ingredient_name": pa.array(df["ingredient_name"], type=pa.string()),
            "usage_count":     pa.array(df["usage_count"],     type=pa.int64()),
            "rank":            pa.array(df["rank"],            type=pa.int32()),
        }
    )
    return table.cast(_GOLD_INGREDIENT_FREQUENCY_PA_SCHEMA)


# ==========================================
# Iceberg write
# ==========================================

def write_to_iceberg(gold_df: pd.DataFrame) -> None:
    """
    gold ingredient_frequency DataFrame을 Iceberg 테이블에 overwrite합니다.

    Args:
        gold_df: aggregate_ingredient_frequency()가 반환한 DataFrame
    """
    if gold_df.empty:
        print("   gold 데이터 없음 — Iceberg write 건너뜀")
        return

    catalog = Iceberg.get_catalog()
    table = catalog.load_table(Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE)
    table.overwrite(_to_arrow_gold(gold_df))
    print(f"   Iceberg overwrite 완료: {Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE} ({len(gold_df)}건)")


# ==========================================
# CSV 저장 (S3 data_csv/)
# ==========================================

def _now_ts() -> str:
    """UTC 기준 타임스탬프 문자열 반환. 예) 20260318_153042"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def write_csv_to_s3(gold_df: pd.DataFrame) -> None:
    """
    gold ingredient_frequency DataFrame을 S3 data_csv/ 폴더에 CSV로 저장합니다.

    저장 경로:
        s3://oliveyoung-crawl-data/data_csv/gold_ingredient_frequency_{YYYYMMDD_HHMMSS}.csv

    Args:
        gold_df: aggregate_ingredient_frequency()가 반환한 DataFrame
    """
    if gold_df.empty:
        print("   gold 데이터 없음 — CSV 저장 건너뜀")
        return

    prefix = S3.DATA_CSV_PATH.removeprefix(f"s3://{S3.BUCKET}/")  # "data_csv/"
    table_name = Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE.split(".")[-1]
    key = f"{prefix}{table_name}_{_now_ts()}.csv"

    buf = io.StringIO()
    gold_df.to_csv(buf, index=False, encoding="utf-8-sig")
    boto3.client("s3", region_name=S3.REGION).put_object(
        Bucket      = S3.BUCKET,
        Key         = key,
        Body        = buf.getvalue().encode("utf-8-sig"),
        ContentType = "text/csv",
    )
    print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")