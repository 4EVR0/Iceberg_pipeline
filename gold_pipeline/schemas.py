"""
Gold 레이어 Iceberg 테이블 스키마 정의

- GOLD_INGREDIENT_FREQUENCY_SCHEMA : 카테고리별 성분 빈도 (batch_date 파티션, append)
- GOLD_PRODUCT_CHANGE_LOG_SCHEMA   : product 엔터티 기준 CDC 변경 로그 (batch_date 파티션)
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    LongType,
    ListType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, DayTransform
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder


# ==========================================
# gold_ingredient_frequency
# ==========================================

GOLD_INGREDIENT_FREQUENCY_SCHEMA = Schema(
    NestedField(1, "category_id",     StringType(),      required=False),
    NestedField(2, "ingredient_name", StringType(),      required=False),
    NestedField(3, "usage_count",     LongType(),        required=False),
    NestedField(4, "rank",            IntegerType(),     required=False),
    NestedField(5, "batch_job",       StringType(),      required=False),
    NestedField(6, "batch_date",      TimestamptzType(), required=False),
)

# batch_date 단위로 파티셔닝 → 날짜별 스냅샷 조회 최적화
GOLD_INGREDIENT_FREQUENCY_PARTITION = PartitionSpec(
    PartitionField(
        source_id=6, field_id=1000,
        transform=DayTransform(), name="batch_date_day",
    )
)

GOLD_INGREDIENT_FREQUENCY_SORT = SortOrder(
    SortField(
        source_id=6, transform=IdentityTransform(),
        direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST,
    )
)


# ==========================================
# gold_product_change_log
# ==========================================
# change_type 값:
#   NEW     — 현재 배치에 새로 등장한 product
#   REMOVED — 이전 배치에 있었으나 현재 배치에서 사라진 product

GOLD_PRODUCT_CHANGE_LOG_SCHEMA = Schema(
    NestedField(1, "batch_date",  TimestamptzType(), required=False),  # 파티션 키
    NestedField(2, "product_id",  StringType(),      required=True),
    NestedField(3, "category_id", StringType(),      required=False),
    NestedField(4, "change_type", StringType(),      required=False),  # NEW | REMOVED
    NestedField(5, "product_name",    StringType(),  required=False),
    NestedField(6, "product_brand",   StringType(),  required=False),
    NestedField(
        7, "product_ingredients",
        ListType(element_id=100, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(8, "batch_job",   StringType(),      required=False),
)

# batch_date 일 단위 파티션
GOLD_PRODUCT_CHANGE_LOG_PARTITION = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000,
        transform=DayTransform(), name="batch_date_day",
    )
)

GOLD_PRODUCT_CHANGE_LOG_SORT = SortOrder(
    SortField(
        source_id=4, transform=IdentityTransform(),
        direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST,
    )
)


# ==========================================
# gold_product_ingredients
# ==========================================

GOLD_PRODUCT_INGREDIENTS_SCHEMA = Schema(
    NestedField(1,  "ingredient_name",      StringType(),      required=False),
    NestedField(2,  "inci_name",            StringType(),      required=False),
    NestedField(3,  "kor_name",             StringType(),      required=False),
    NestedField(4,  "eng_name",             StringType(),      required=False),
    NestedField(5,  "cosing_functions",     StringType(),      required=False),
    NestedField(6,  "status",               StringType(),      required=False),
    NestedField(7,  "cosmetic_restriction", StringType(),      required=False),
    NestedField(8,  "other_restrictions",   StringType(),      required=False),
    NestedField(9,  "usage_count",          LongType(),        required=False),
    NestedField(10, "batch_job",            StringType(),      required=False),
    NestedField(11, "batch_date",           TimestamptzType(), required=False),
)

# batch_date 일 단위 파티션 → 날짜별 스냅샷 조회 최적화
GOLD_PRODUCT_INGREDIENTS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=11, field_id=1001,
        transform=DayTransform(), name="batch_date_day",
    )
)

GOLD_PRODUCT_INGREDIENTS_SORT = SortOrder(
    SortField(
        source_id=11, transform=IdentityTransform(),
        direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST,
    )
)
