"""
Reference Data JSON → Iceberg 동기화

JSON 파일(Git 관리)을 수정한 뒤 이 스크립트를 실행하면
Iceberg 테이블에 전체 overwrite로 반영됩니다.

실행:
    cd Iceberg_pipeline
    python reference_pipeline/sync_reference_data.py            # 전체 sync
    python reference_pipeline/sync_reference_data.py --typo     # typo_map만
    python reference_pipeline/sync_reference_data.py --garbage  # garbage_keywords만
"""

import sys
import os
import json
import argparse
from datetime import datetime, timezone

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pyarrow as pa

from config.settings import DataPath, Iceberg


# ==========================================
# 내부 헬퍼
# ==========================================

def _load_json(path: str) -> list | dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _to_arrow(table, df_dict: dict) -> pa.Table:
    """Iceberg 테이블 스키마 기준으로 Arrow Table을 생성합니다."""
    arrow_schema = table.schema().as_arrow()
    return pa.table(df_dict, schema=arrow_schema)


# ==========================================
# sync 함수
# ==========================================

def sync_typo_map(catalog) -> None:
    """
    typo_map.json + typo_map_regex.json → Iceberg typo_map 테이블 overwrite

    match_type:
        'simple'         → typo_map.json       (단순 str.replace)
        'regex_boundary' → typo_map_regex.json (경계 패턴 정규식)
    """
    simple_entries = _load_json(DataPath.TYPO_MAP_JSON)
    regex_entries  = _load_json(DataPath.TYPO_MAP_REGEX_JSON)

    synced_at = datetime.now(timezone.utc)

    raws        = []
    fixes       = []
    match_types = []

    for e in simple_entries:
        raws.append(e["raw"])
        fixes.append(e["fix"])
        match_types.append("simple")

    for e in regex_entries:
        raws.append(e["raw"])
        fixes.append(e["fix"])
        match_types.append("regex_boundary")

    synced_ats = [synced_at] * len(raws)

    table = catalog.load_table(Iceberg.TYPO_MAP_TABLE)
    arrow_table = _to_arrow(table, {
        "raw":        pa.array(raws,        type=pa.string()),
        "fix":        pa.array(fixes,       type=pa.string()),
        "match_type": pa.array(match_types, type=pa.string()),
        "synced_at":  pa.array(synced_ats,  type=pa.timestamp("us", tz="UTC")),
    })

    table.overwrite(arrow_table)
    print(f"   typo_map sync 완료: {len(raws)}건 "
          f"(simple={len(simple_entries)}, regex_boundary={len(regex_entries)})")


def sync_garbage_keywords(catalog) -> None:
    """
    garbage_keywords.json → Iceberg garbage_keywords 테이블 overwrite

    match_type:
        'exact'    → 완전 일치
        'contains' → 포함 여부
    """
    config = _load_json(DataPath.GARBAGE_KEYWORDS_JSON)

    synced_at = datetime.now(timezone.utc)

    match_types = []
    keywords    = []

    for kw in config.get("exact", []):
        match_types.append("exact")
        keywords.append(kw)

    for kw in config.get("contains", []):
        match_types.append("contains")
        keywords.append(kw)

    synced_ats = [synced_at] * len(keywords)

    table = catalog.load_table(Iceberg.GARBAGE_KEYWORDS_TABLE)
    arrow_table = _to_arrow(table, {
        "match_type": pa.array(match_types, type=pa.string()),
        "keyword":    pa.array(keywords,    type=pa.string()),
        "synced_at":  pa.array(synced_ats,  type=pa.timestamp("us", tz="UTC")),
    })

    table.overwrite(arrow_table)
    print(f"   garbage_keywords sync 완료: {len(keywords)}건 "
          f"(exact={len(config.get('exact', []))}, contains={len(config.get('contains', []))})")


# ==========================================
# 실행 진입점
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--typo",    action="store_true", help="typo_map만 sync")
    parser.add_argument("--garbage", action="store_true", help="garbage_keywords만 sync")
    args = parser.parse_args()

    run_all    = not args.typo and not args.garbage
    run_typo   = run_all or args.typo
    run_garbage = run_all or args.garbage

    print("=== Reference Data sync ===\n")

    catalog = Iceberg.get_catalog()

    if run_typo:
        print(f"1. {Iceberg.TYPO_MAP_TABLE}")
        sync_typo_map(catalog)

    if run_garbage:
        print(f"\n2. {Iceberg.GARBAGE_KEYWORDS_TABLE}")
        sync_garbage_keywords(catalog)

    print("\n=== 완료 ===")
