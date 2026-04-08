"""
oliveyoung_silver / oliveyoung_silver_error Iceberg 테이블 생성

실행 (테이블이 없을 때 최초 1회):
    cd Iceberg_pipeline
    python silver_pipeline/create_silver.py

테이블을 재생성하려면:
    python silver_pipeline/create_silver.py --recreate
"""

import sys
import os
import argparse

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pyiceberg.table.sorting import SortOrder

from config.settings import S3, Iceberg
from silver_pipeline.schemas import (
    SILVER_SCHEMA,
    SILVER_ERROR_SCHEMA,
    SILVER_PARTITION,
    SILVER_ERROR_PARTITION,
    SILVER_SORT_ORDER,
)


# ==========================================
# 테이블 생성 함수
# ==========================================

def create_table_if_not_exists(catalog, table_name, schema, partition_spec, sort_order, location):
    db, _ = table_name.split(".", 1)
    try:
        table = catalog.load_table(table_name)
        print(f"   이미 존재: {table_name} (건너뜀)")
        return table, False
    except Exception:
        pass

    namespaces = [ns[0] for ns in catalog.list_namespaces()]
    if db not in namespaces:
        try:
            catalog.create_namespace(db)
            print(f"   네임스페이스 생성: {db}")
        except Exception as e:
            print(f"   네임스페이스 생성 시도 중 참고: {e}")

    table = catalog.create_table(
        identifier     = table_name,
        schema         = schema,
        location       = location,
        partition_spec = partition_spec,
        sort_order     = sort_order,
    )
    print(f"   테이블 생성 완료: {table_name}")
    return table, True


def drop_and_recreate(catalog, table_name, schema, partition_spec, sort_order, location):
    try:
        catalog.drop_table(table_name)
        print(f"   기존 테이블 삭제: {table_name}")
    except Exception:
        pass
    return create_table_if_not_exists(
        catalog, table_name, schema, partition_spec, sort_order, location
    )


# ==========================================
# 실행 진입점
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recreate", action="store_true",
                        help="기존 테이블을 삭제하고 재생성합니다.")
    args = parser.parse_args()

    print("=== Silver 테이블 생성 ===\n")

    catalog = Iceberg.get_catalog()
    fn = drop_and_recreate if args.recreate else create_table_if_not_exists

    print(f"1. {Iceberg.SILVER_CURRENT_TABLE}")
    fn(catalog,
       table_name     = Iceberg.SILVER_CURRENT_TABLE,
       schema         = SILVER_SCHEMA,
       partition_spec = SILVER_PARTITION,
       sort_order     = SILVER_SORT_ORDER,
       location       = S3.SILVER_CURRENT_PATH)

    print(f"\n2. {Iceberg.SILVER_HISTORY_TABLE}")
    fn(catalog,
       table_name     = Iceberg.SILVER_HISTORY_TABLE,
       schema         = SILVER_SCHEMA,
       partition_spec = SILVER_PARTITION,
       sort_order     = SILVER_SORT_ORDER,
       location       = S3.SILVER_HISTORY_PATH)

    print(f"\n3. {Iceberg.SILVER_ERROR_TABLE}")
    fn(catalog,
       table_name     = Iceberg.SILVER_ERROR_TABLE,
       schema         = SILVER_ERROR_SCHEMA,
       partition_spec = SILVER_ERROR_PARTITION,
       sort_order     = SortOrder(),
       location       = S3.SILVER_ERROR_PATH)

    print("\n=== 완료 ===")
