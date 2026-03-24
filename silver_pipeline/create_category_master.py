"""
카테고리 테이블 생성 스크립트 (최초 1회, 혹은 이후 수정 시 실행)
- 카테고리 데이터를 DataFrame 형태로 생성 및 S3에 저장
- iceberg 메타데이터(스냅샷) 생성 (oliveyoung_category_master)

실행:
    cd Iceberg_pipeline
    python silver_pipeline/create_category_master.py

테이블을 재생성하려면:
    python silver_pipeline/create_category_master.py --recreate
"""

import sys
import os
import argparse

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder

from config.settings import S3, Iceberg


# ==========================================
# 카테고리 데이터 정의
# ==========================================

CATEGORIES = {
    "스킨케어":     ["스킨/토너", "에센스/세럼/앰플", "크림", "로션", "미스트/오일"],
    "마스크팩":     ["시트팩", "패드", "페이셜팩", "코팩", "패치"],
    "클렌징":       ["클렌징폼/젤", "오일/밤", "워터/밀크", "필링&스크럽"],
    "더모 코스메틱": ["스킨케어", "바디케어", "클렌징", "선케어", "마스크팩"],
    "맨즈케어":     ["스킨케어"],
}

CATEGORY_MASTER_SCHEMA = Schema(
    NestedField(1, "category_id",    StringType(), required=True),
    NestedField(2, "main_category",  StringType(), required=False),
    NestedField(3, "sub_category",   StringType(), required=False),
)


# ==========================================
# 헬퍼
# ==========================================

_CATEGORY_MASTER_PA_SCHEMA = pa.schema([
    pa.field("category_id",   pa.string(), nullable=False),  # required
    pa.field("main_category", pa.string(), nullable=True),
    pa.field("sub_category",  pa.string(), nullable=True),
])

def build_arrow_table() -> pa.Table:
    flat_data = []
    for main, subs in CATEGORIES.items():
        for sub in subs:
            cat_id = f"{main}_{sub}".replace(" ", "").replace("/", "-")
            flat_data.append({
                "category_id":   cat_id,
                "main_category": main,
                "sub_category":  sub,
            })
    return pa.Table.from_pandas(pd.DataFrame(flat_data), schema=_CATEGORY_MASTER_PA_SCHEMA)


def create_table_if_not_exists(catalog):
    try:
        table = catalog.load_table(Iceberg.CATEGORY_MASTER_TABLE)
        print(f"   이미 존재: {Iceberg.CATEGORY_MASTER_TABLE} (건너뜀)")
        return table, False
    except Exception:
        pass

    db = Iceberg.DATABASE
    namespaces = [ns[0] for ns in catalog.list_namespaces()]
    if db not in namespaces:
        try:
            catalog.create_namespace(db)
            print(f"   네임스페이스 생성: {db}")
        except Exception as e:
            print(f"   네임스페이스 생성 시도 중 참고: {e}")

    table = catalog.create_table(
        identifier     = Iceberg.CATEGORY_MASTER_TABLE,
        schema         = CATEGORY_MASTER_SCHEMA,
        location       = S3.CATEGORY_MASTER_PATH,
        partition_spec = PartitionSpec(),   # 파티셔닝 없음 (행 수가 적음)
        sort_order     = SortOrder(),
    )
    print(f"   테이블 생성 완료: {Iceberg.CATEGORY_MASTER_TABLE}")
    return table, True


def drop_and_recreate(catalog):
    try:
        catalog.drop_table(Iceberg.CATEGORY_MASTER_TABLE)
        print(f"   기존 테이블 삭제: {Iceberg.CATEGORY_MASTER_TABLE}")
    except Exception:
        pass
    return create_table_if_not_exists(catalog)


# ==========================================
# 실행 진입점
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recreate", action="store_true",
                        help="기존 테이블을 삭제하고 재생성합니다.")
    args = parser.parse_args()

    print("=== Category Master 테이블 생성 ===\n")

    catalog = Iceberg.get_catalog()
    fn = drop_and_recreate if args.recreate else create_table_if_not_exists
    table, created = fn(catalog)

    arrow_table = build_arrow_table()
    if created:
        table.append(arrow_table)
        print(f"   데이터 적재 완료: {len(arrow_table)}건")
    else:
        print(f"   데이터 적재 건너뜀 — --recreate 옵션으로 재실행하세요.")

    print("\n=== 완료 ===")