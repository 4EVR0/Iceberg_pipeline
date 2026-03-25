"""
oliveyoung_gold_ingredient_frequency Iceberg 테이블 생성

실행 (테이블이 없을 때 최초 1회):
    cd Iceberg_pipeline
    python gold_pipeline/create_gold.py

테이블을 재생성하려면:
    python gold_pipeline/create_gold.py --recreate
"""

import sys
import os
import argparse

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pyiceberg.schema        import Schema
from pyiceberg.types         import NestedField, StringType, IntegerType, LongType
from pyiceberg.partitioning  import PartitionSpec, PartitionField
from pyiceberg.transforms    import IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder

from config.settings import S3, Iceberg


# ==========================================
# 스키마 정의
# ==========================================

GOLD_INGREDIENT_FREQUENCY_SCHEMA = Schema(
    NestedField(1, "category_id",     StringType(),  required=False),
    NestedField(2, "ingredient_name", StringType(),  required=False),
    NestedField(3, "usage_count",     LongType(),    required=False),
    NestedField(4, "rank",            IntegerType(), required=False),
)


# ==========================================
# 파티션 / 정렬 설정
# ==========================================

# category_id 파티셔닝 (카테고리별 TOP 50 조회 최적화)
GOLD_PARTITION = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000,
        transform=IdentityTransform(), name="category_id",
    )
)

# rank 오름차순 정렬
GOLD_SORT_ORDER = SortOrder(
    SortField(
        source_id=4, transform=IdentityTransform(),
        direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST,
    )
)


# ==========================================
# 테이블 생성 함수
# ==========================================

def create_table_if_not_exists(catalog):
    try:
        table = catalog.load_table(Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE)
        print(f"   이미 존재: {Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE} (건너뜀)")
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
        identifier     = Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE,
        schema         = GOLD_INGREDIENT_FREQUENCY_SCHEMA,
        location       = S3.INGREDIENT_FREQUENCY_PATH,
        partition_spec = GOLD_PARTITION,
        sort_order     = GOLD_SORT_ORDER,
    )
    print(f"   테이블 생성 완료: {Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE}")
    return table, True


def drop_and_recreate(catalog):
    try:
        catalog.drop_table(Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE)
        print(f"   기존 테이블 삭제: {Iceberg.GOLD_INGREDIENT_FREQUENCY_TABLE}")
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

    print("=== Gold 테이블 생성 ===\n")

    catalog = Iceberg.get_catalog()
    fn = drop_and_recreate if args.recreate else create_table_if_not_exists
    fn(catalog)

    print("\n=== 완료 ===")