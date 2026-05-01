"""
CDC (Change Data Capture) 모듈

silver_history에서 최신 2개 batch_date 스냅샷을 비교해
product 엔터티 기준 변경분(NEW / REMOVED)을 추출합니다.

흐름:
    1. silver_history 전체 스캔 → pandas
    2. 최신 2개 batch_date 추출 (prev, curr)
    3. product_id 집합 비교
       - NEW     : curr에 있고 prev에 없는 product
       - REMOVED : prev에 있고 curr에 없는 product
    4. 변경 레코드 DataFrame 반환 (write_gold.py 에서 Iceberg append)

반환:
    pd.DataFrame | None
        변경 레코드가 없으면 None 반환
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# silver_history에서 CDC에 필요한 컬럼만 선택
_REQUIRED_COLS = [
    "batch_date",
    "product_id",
    "category_id",
    "product_name",
    "product_brand",
    "product_ingredients",
]


def _load_history(catalog) -> pd.DataFrame:
    """silver_history 테이블을 pandas DataFrame으로 로드합니다."""
    from config.settings import Iceberg

    table = catalog.load_table(Iceberg.SILVER_HISTORY_TABLE)
    df = (
        table.scan(selected_fields=tuple(_REQUIRED_COLS))
        .to_arrow()
        .to_pandas()
    )
    logger.info(f"silver_history 로드: {len(df)}건")
    return df


def _pick_two_latest_batch_dates(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """
    DataFrame에서 batch_date 컬럼 기준 최신 2개 날짜를 반환합니다.

    Returns:
        (prev_date, curr_date) 또는 스냅샷이 1개 이하이면 None
    """
    dates = (
        df["batch_date"]
        .dropna()
        .drop_duplicates()
        .sort_values(ascending=False)
        .reset_index(drop=True)
    )

    if len(dates) < 2:
        logger.warning(
            f"silver_history 스냅샷이 {len(dates)}개뿐입니다. "
            "CDC를 실행하려면 최소 2개의 batch_date가 필요합니다."
        )
        return None

    curr_date = dates.iloc[0]
    prev_date = dates.iloc[1]
    logger.info(f"비교 대상 — prev: {prev_date}  curr: {curr_date}")
    return prev_date, curr_date


def compute_change_log(catalog, batch_job: str) -> pd.DataFrame | None:
    """
    silver_history 최신 2개 스냅샷을 비교하여 변경 레코드 DataFrame을 반환합니다.

    Args:
        catalog  : pyiceberg Catalog 인스턴스
        batch_job: 현재 배치 식별자 (gold_product_change_log.batch_job 컬럼에 기록)

    Returns:
        변경 레코드 DataFrame(컬럼: batch_date, product_id, category_id, change_type,
                              product_name, product_brand, product_ingredients, batch_job)
        변경 레코드가 없으면 None
    """
    history_df = _load_history(catalog)

    result = _pick_two_latest_batch_dates(history_df)
    if result is None:
        return None

    prev_date, curr_date = result

    prev_df = history_df[history_df["batch_date"] == prev_date].copy()
    curr_df = history_df[history_df["batch_date"] == curr_date].copy()

    prev_ids = set(prev_df["product_id"].dropna())
    curr_ids = set(curr_df["product_id"].dropna())

    new_ids     = curr_ids - prev_ids
    removed_ids = prev_ids - curr_ids

    logger.info(f"NEW: {len(new_ids)}건  REMOVED: {len(removed_ids)}건")

    if not new_ids and not removed_ids:
        logger.info("변경 레코드 없음 — CDC 건너뜀")
        return None

    rows = []

    # NEW: curr 스냅샷에서 해당 product 정보 가져오기
    if new_ids:
        new_df = curr_df[curr_df["product_id"].isin(new_ids)].copy()
        new_df["change_type"] = "NEW"
        new_df["batch_date"]  = curr_date
        rows.append(new_df)

    # REMOVED: prev 스냅샷에서 해당 product 정보 가져오기
    if removed_ids:
        removed_df = prev_df[prev_df["product_id"].isin(removed_ids)].copy()
        removed_df["change_type"] = "REMOVED"
        removed_df["batch_date"]  = curr_date  # 로그 기록 시점은 현재 배치 기준
        rows.append(removed_df)

    change_df = pd.concat(rows, ignore_index=True)
    change_df["batch_job"] = batch_job

    # gold_product_change_log 스키마 컬럼 순서에 맞게 정렬
    output_cols = [
        "batch_date",
        "product_id",
        "category_id",
        "change_type",
        "product_name",
        "product_brand",
        "product_ingredients",
        "batch_job",
    ]
    change_df = change_df[output_cols].reset_index(drop=True)

    logger.info(f"CDC 변경 레코드 생성: {len(change_df)}건")
    return change_df
