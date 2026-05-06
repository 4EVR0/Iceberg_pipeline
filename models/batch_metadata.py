from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd


@dataclass(frozen=True)
class BatchMetadata:
    batch_job: str
    batch_date: datetime


def create_batch_metadata() -> BatchMetadata:
    """
    UTC 기준의 배치 메타데이터를 생성합니다.

    batch_job  : YYYYMMDD_HHMMSS
    batch_date : timezone-aware UTC datetime
    """
    batch_date = datetime.now(timezone.utc)
    return BatchMetadata(
        batch_job=batch_date.strftime("%Y%m%d_%H%M%S"),
        batch_date=batch_date,
    )


def add_batch_metadata(df: pd.DataFrame, batch: BatchMetadata) -> pd.DataFrame:
    """
    DataFrame에 표준 batch_job, batch_date 컬럼을 추가합니다.
    빈 DataFrame은 그대로 반환합니다.
    """
    if df.empty:
        return df

    df["batch_job"] = batch.batch_job
    df["batch_date"] = pd.Timestamp(batch.batch_date)
    return df
