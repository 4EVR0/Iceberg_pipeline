"""
Silver / Silver Error Iceberg 테이블 스키마 정의

- 스키마(Schema), 파티션(PartitionSpec), 정렬(SortOrder) 을 한 곳에서 관리합니다.
- create_silver.py 는 이 파일에서 import해서 테이블 생성에 사용합니다.
- write_silver.py 는 table.schema().as_arrow() 로 런타임 스키마를 참조하므로
  이 파일을 직접 사용하지 않습니다.
"""

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


# ==========================================
# 공통 타입
# ==========================================

# review_stats: map<string, map<string, string>>
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


# ==========================================
# 스키마
# ==========================================

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
    NestedField(1,  "category_id",             StringType(),      required=False),
    NestedField(2,  "product_id",              StringType(),      required=True),
    NestedField(3,  "product_brand",           StringType(),      required=False),
    NestedField(4,  "product_name_raw",        StringType(),      required=False),
    NestedField(5,  "product_name",            StringType(),      required=False),
    NestedField(6,  "product_ingredients_raw", StringType(),      required=False),
    NestedField(7,  "product_url",             StringType(),      required=False),
    NestedField(8,  "crawled_at",              TimestamptzType(), required=False),
    NestedField(9,  "error_type",              StringType(),      required=False),
    NestedField(10, "residual_text",           StringType(),      required=False),
    NestedField(11, "batch_job",               StringType(),      required=False),
    NestedField(12, "batch_date",              TimestamptzType(), required=False),
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
        source_id=9, field_id=1000,
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
