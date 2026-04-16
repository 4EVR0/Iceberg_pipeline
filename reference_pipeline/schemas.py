"""
Reference Data Iceberg 테이블 스키마 정의

- typo_map:         오타/유의어 매핑 (typo_map.json + typo_map_regex.json 통합)
- garbage_keywords: 가비지 제품명 필터링 키워드 (garbage_keywords.json)
"""

from pyiceberg.schema       import Schema
from pyiceberg.types        import NestedField, StringType, TimestamptzType
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder


# ==========================================
# typo_map
# ==========================================
# match_type:
#   'simple'         → typo_map.json       (단순 str.replace)
#   'regex_boundary' → typo_map_regex.json (경계 패턴 정규식 치환)

TYPO_MAP_SCHEMA = Schema(
    NestedField(1, "raw",        StringType(),      required=True),
    NestedField(2, "fix",        StringType(),      required=True),
    NestedField(3, "match_type", StringType(),      required=True),
    NestedField(4, "synced_at",  TimestamptzType(), required=False),
)


# ==========================================
# garbage_keywords
# ==========================================
# match_type:
#   'exact'    → 제품명 완전 일치 시 garbage
#   'contains' → 제품명 포함 시 garbage

GARBAGE_KEYWORDS_SCHEMA = Schema(
    NestedField(1, "match_type", StringType(),      required=True),
    NestedField(2, "keyword",    StringType(),      required=True),
    NestedField(3, "synced_at",  TimestamptzType(), required=False),
)


# ==========================================
# 파티션 / 정렬 (Reference 테이블은 소규모 — 파티션 없음)
# ==========================================

REFERENCE_PARTITION  = PartitionSpec()
REFERENCE_SORT_ORDER = SortOrder()
