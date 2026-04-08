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

from pyiceberg.schema        import Schema
from pyiceberg.types         import (
    NestedField,
    StringType,
    FloatType,
    IntegerType,
    ListType,
    MapType,
    TimestamptzType,
)
from pyiceberg.partitioning  import PartitionSpec, PartitionField
from pyiceberg.transforms    import IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder

from config.settings import S3, Iceberg


# ==========================================
# 스키마 정의
# ==========================================

# review_stats 타입: map<string, map<string, string>>
# 예) {"피부타입": {"복합성에 좋아요": "54%", ...}, "세정력": {...}}
REVIEW_STATS_TYPE = MapType(
    key_id=101, key_type=StringType(),
    value_id=102, value_type=MapType(
        key_id=103, key_type=StringType(),
        value_id=104, value_type=StringType(),
        value_required=False,
    ),
    value_required=False,
)

SILVER_SCHEMA = Schema(
    NestedField(1,  "category_id",             StringType(),      required=False),
    NestedField(2,  "product_id",              StringType(),      required=True),
    NestedField(3,  "product_brand",           StringType(),      required=False),
    NestedField(4,  "product_name",            StringType(),      required=False),
    NestedField(5,  "product_name_raw",        StringType(),      required=False),
    NestedField(6,  "product_ingredients",
        ListType(element_id=100, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(7,  "product_ingredients_raw", StringType(),      required=False),
    NestedField(8,  "rating",                  FloatType(),       required=False),
    NestedField(9,  "review_count",            IntegerType(),     required=False),
    NestedField(10, "review_stats",            REVIEW_STATS_TYPE, required=False),
    NestedField(11, "product_url",             StringType(),      required=False),
    NestedField(12, "crawled_at",              TimestamptzType(), required=False),
    NestedField(13, "batch_job",               StringType(),      required=False),
    NestedField(14, "batch_date",              TimestamptzType(), required=False),
)

# DLQ 패턴: 에러 원인 추적에 필요한 컬럼만 유지
SILVER_ERROR_SCHEMA = Schema(
    NestedField(1, "category_id",             StringType(),    required=False),
    NestedField(2, "product_id",              StringType(),    required=True),
    NestedField(3, "product_brand",           StringType(),    required=False),
    NestedField(4, "product_name_raw",        StringType(),    required=False),
    NestedField(4, "product_name",            StringType(),    required=False),
    NestedField(5, "product_ingredients_raw", StringType(),    required=False),
    NestedField(6, "product_url",             StringType(),    required=False),
    NestedField(7, "crawled_at",              TimestamptzType(), required=False),
    NestedField(8,  "error_type",              StringType(),    required=False),
    NestedField(9,  "residual_text",           StringType(),    required=False),
    NestedField(10, "batch_job",               StringType(),    required=False),
    NestedField(11, "batch_date",              TimestamptzType(), required=False),
)


# ==========================================
# 파티션 / 정렬 설정
# ==========================================

# silver: category_id 파티셔닝 (카테고리별 조회 최적화)
SILVER_PARTITION = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000,
        transform=IdentityTransform(), name="category_id",
    )
)

# silver_error: error_type 파티셔닝 (에러 유형별 재처리 최적화)
SILVER_ERROR_PARTITION = PartitionSpec(
    PartitionField(
        source_id=8, field_id=1000,
        transform=IdentityTransform(), name="error_type",
    )
)

# silver: crawled_at 오름차순 정렬
SILVER_SORT_ORDER = SortOrder(
    SortField(
        source_id=12, transform=IdentityTransform(),
        direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST,
    )
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
            # 이미 존재하는 경우 등의 예외 처리
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