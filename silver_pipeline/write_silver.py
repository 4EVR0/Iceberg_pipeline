"""
Silver / Silver Error 테이블 Iceberg write + CSV 저장 모듈
"""

import io
import json
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.lib
from pyiceberg.catalog import load_catalog

from config.settings import S3, Iceberg


# ==========================================
# Iceberg write
# ==========================================

def _get_catalog():
    return load_catalog(
        Iceberg.CATALOG_NAME,
        **{
            "type":      "glue",
            "warehouse": S3.ICEBERG_METADATA_PATH,
            "s3.region": S3.REGION,
        }
    )


# review_stats 컬럼 타입 정의 (Iceberg 스키마와 일치: map<string, map<string, string>>)
_REVIEW_STATS_PA_TYPE = pa.map_(
    pa.string(),
    pa.map_(pa.string(), pa.string()),
)


# Iceberg silver 테이블 스키마와 1:1 대응하는 PyArrow 스키마
# product_id만 required(not null), 나머지는 optional(nullable)
_SILVER_PA_SCHEMA = pa.schema([
    pa.field("category_id",             pa.string(),    nullable=True),
    pa.field("product_id",              pa.string(),    nullable=False),  # required
    pa.field("product_brand",           pa.string(),    nullable=True),
    pa.field("product_name",            pa.string(),    nullable=True),
    pa.field("product_name_raw",        pa.string(),    nullable=True),
    pa.field("product_ingredients",
        pa.list_(pa.string()),          nullable=True),
    pa.field("product_ingredients_raw", pa.string(),    nullable=True),
    pa.field("rating",                  pa.float32(),   nullable=True),
    pa.field("review_count",            pa.int32(),     nullable=True),
    pa.field("review_stats",            _REVIEW_STATS_PA_TYPE, nullable=True),
    pa.field("product_url",             pa.string(),    nullable=True),
    pa.field("crawled_at",              pa.timestamp("us", tz="UTC"), nullable=True),
])


# Iceberg silver_error 테이블 스키마 (product_id만 required)
_SILVER_ERROR_PA_SCHEMA = pa.schema([
    pa.field("category_id",             pa.string(),        nullable=True),
    pa.field("product_id",              pa.string(),        nullable=False),
    pa.field("product_brand",           pa.string(),        nullable=True),
    pa.field("product_name_raw",        pa.string(),        nullable=True),
    pa.field("product_ingredients_raw", pa.string(),        nullable=True),
    pa.field("product_url",             pa.string(),        nullable=True),
    pa.field("crawled_at",              pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("error_type",              pa.string(),        nullable=True),
    pa.field("residual_text",           pa.string(),        nullable=True),
])


def _to_arrow_silver(df: pd.DataFrame) -> pa.Table:
    """
    silver DataFrame을 Iceberg 스키마에 맞는 PyArrow Table로 변환합니다.
    """
    def _parse_review_stats(val):
        if not val:
            return {}
        result = {}
        for category, items in val.items():
            result[category] = dict(items) if items is not None else {}
        return result
 
    review_stats_col = pa.array(
        [_parse_review_stats(val) for val in df["review_stats"].tolist()],
        type=_REVIEW_STATS_PA_TYPE,
    )

    table = pa.table(
        {
            "category_id":             pa.array(df["category_id"],             type=pa.string()),
            "product_id":              pa.array(df["product_id"],              type=pa.string()),
            "product_brand":           pa.array(df["product_brand"],           type=pa.string()),
            "product_name":            pa.array(df["product_name"],            type=pa.string()),
            "product_name_raw":        pa.array(df["product_name_raw"],        type=pa.string()),
            "product_ingredients":     pa.array(df["product_ingredients"].tolist(), type=pa.list_(pa.string())),
            "product_ingredients_raw": pa.array(df["product_ingredients_raw"], type=pa.string()),
            "rating":                  pa.array(df["rating"],                  type=pa.float32()),
            "review_count":            pa.array(df["review_count"],            type=pa.int32()),
            "review_stats":            review_stats_col,
            "product_url":             pa.array(df["product_url"],             type=pa.string()),
            "crawled_at":              pa.array(
                pd.to_datetime(df["crawled_at"], utc=True),
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
    )
    # pa.table()은 nullable을 재추론하므로 product_id required 보장을 위해 cast로 강제 적용
    return table.cast(_SILVER_PA_SCHEMA)


def _to_arrow_error(df: pd.DataFrame) -> pa.Table:
    """
    error DataFrame을 Iceberg 스키마에 맞는 PyArrow Table로 변환합니다.
    """
    table = pa.table(
        {
            "category_id":             pa.array(df["category_id"],             type=pa.string()),
            "product_id":              pa.array(df["product_id"],              type=pa.string()),
            "product_brand":           pa.array(df["product_brand"],           type=pa.string()),
            "product_name_raw":        pa.array(df["product_name_raw"],        type=pa.string()),
            "product_ingredients_raw": pa.array(df["product_ingredients_raw"], type=pa.string()),
            "product_url":             pa.array(df["product_url"],             type=pa.string()),
            "crawled_at":              pa.array(
                pd.to_datetime(df["crawled_at"], utc=True),
                type=pa.timestamp("us", tz="UTC"),
            ),
            "error_type":              pa.array(df["error_type"],              type=pa.string()),
            "residual_text":           pa.array(df["residual_text"],           type=pa.string()),
        },
    )
    return table.cast(_SILVER_ERROR_PA_SCHEMA)


def write_to_iceberg(silver_df: pd.DataFrame, error_df: pd.DataFrame) -> None:
    """
    silver / error DataFrame을 각 Iceberg 테이블에 overwrite 합니다.

    Args:
        silver_df: process_pipeline()이 반환한 silver DataFrame
        error_df:  process_pipeline()이 반환한 error DataFrame
    """
    catalog = _get_catalog()

    if not silver_df.empty:
        table = catalog.load_table(Iceberg.SILVER_TABLE)
        table.overwrite(_to_arrow_silver(silver_df))
        print(f"   Iceberg overwrite 완료: {Iceberg.SILVER_TABLE} ({len(silver_df)}건)")
    else:
        print(f"   silver 데이터 없음 — Iceberg write 건너뜀")

    if not error_df.empty:
        table = catalog.load_table(Iceberg.SILVER_ERROR_TABLE)
        table.overwrite(_to_arrow_error(error_df))
        print(f"   Iceberg overwrite 완료: {Iceberg.SILVER_ERROR_TABLE} ({len(error_df)}건)")
    else:
        print(f"   error 데이터 없음 — Iceberg write 건너뜀")


# ==========================================
# CSV 저장 (S3 data_csv/)
# ==========================================

def _now_ts() -> str:
    """UTC 기준 타임스탬프 문자열 반환. 예) 20260318_153042"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _upload_csv(df: pd.DataFrame, s3_key: str) -> None:
    """DataFrame을 CSV로 직렬화하여 S3에 업로드합니다."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    boto3.client("s3", region_name=S3.REGION).put_object(
        Bucket = S3.BUCKET,
        Key    = s3_key,
        Body   = buf.getvalue().encode("utf-8-sig"),
        ContentType = "text/csv",
    )


def write_csv_to_s3(silver_df: pd.DataFrame, error_df: pd.DataFrame) -> None:
    """
    silver / error DataFrame을 S3 data_csv/ 폴더에 CSV로 저장합니다.

    저장 경로:
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_{YYYYMMDD_HHMMSS}.csv
        s3://oliveyoung-crawl-data/data_csv/olive_young_silver_error_{YYYYMMDD_HHMMSS}.csv

    Args:
        silver_df: process_pipeline()이 반환한 silver DataFrame
        error_df:  process_pipeline()이 반환한 error DataFrame
    """
    ts = _now_ts()
    prefix = S3.DATA_CSV_PATH.removeprefix(f"s3://{S3.BUCKET}/")  # "data_csv/"

    if not silver_df.empty:
        # product_ingredients(list)는 CSV에서 문자열로 직렬화
        csv_df = silver_df.copy()
        csv_df["product_ingredients"] = csv_df["product_ingredients"].apply(
            lambda v: "|".join(v) if isinstance(v, list) else v
        )
        silver_table_name = Iceberg.SILVER_TABLE.split(".")[-1]  # "olive_young_silver"
        key = f"{prefix}{silver_table_name}_{ts}.csv"
        _upload_csv(csv_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")

    if not error_df.empty:
        error_table_name = Iceberg.SILVER_ERROR_TABLE.split(".")[-1]
        key = f"{prefix}{error_table_name}_{ts}.csv"
        _upload_csv(error_df, key)
        print(f"   CSV 저장 완료: s3://{S3.BUCKET}/{key}")