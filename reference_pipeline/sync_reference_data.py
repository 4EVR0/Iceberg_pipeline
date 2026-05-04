"""
Reference Data JSON → Iceberg 동기화

JSON 파일(Git 관리)을 수정한 뒤 이 스크립트를 실행하면
Iceberg 테이블에 전체 overwrite로 반영됩니다.

실행:
    cd Iceberg_pipeline
    python reference_pipeline/sync_reference_data.py
"""

import sys
import os
import json
from datetime import datetime, timezone

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pyarrow as pa
from config.settings import DataPath, OliveyoungIceberg



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
    typo_map.json + typo_map_regex.json + product_name_norm_map.json
    → Iceberg typo_map 테이블 전체 overwrite

    apply_to='ingredient':
        'simple'         → typo_map.json       (단순 str.replace)
        'regex_boundary' → typo_map_regex.json (경계 패턴 정규식)
    apply_to='product_name':
        'regex'          → product_name_norm_map.json (경계 없는 정규식)
        'simple'         → product_name_norm_map.json (단순 str.replace)
    """
    simple_entries = _load_json(DataPath.TYPO_MAP_JSON)
    regex_entries  = _load_json(DataPath.TYPO_MAP_REGEX_JSON)
    norm_entries   = _load_json(DataPath.PRODUCT_NAME_NORM_MAP_JSON)

    synced_at = datetime.now(timezone.utc)

    raws        = []
    fixes       = []
    match_types = []
    apply_tos   = []

    for e in simple_entries:
        raws.append(e["raw"])
        fixes.append(e["fix"])
        match_types.append("simple")
        apply_tos.append("ingredient")

    for e in regex_entries:
        raws.append(e["raw"])
        fixes.append(e["fix"])
        match_types.append("regex_boundary")
        apply_tos.append("ingredient")

    for e in norm_entries:
        raws.append(e["raw"])
        fixes.append(e["fix"])
        match_types.append(e["match_type"])
        apply_tos.append("product_name")

    synced_ats = [synced_at] * len(raws)

    table = catalog.load_table(OliveyoungIceberg.TYPO_MAP_TABLE)
    arrow_table = _to_arrow(table, {
        "raw":        pa.array(raws,        type=pa.string()),
        "fix":        pa.array(fixes,       type=pa.string()),
        "match_type": pa.array(match_types, type=pa.string()),
        "synced_at":  pa.array(synced_ats,  type=pa.timestamp("us", tz="UTC")),
        "apply_to":   pa.array(apply_tos,   type=pa.string()),
    })

    table.overwrite(arrow_table)
    print(f"   typo_map sync 완료: {len(raws)}건 "
          f"(ingredient simple={len(simple_entries)}, "
          f"ingredient regex_boundary={len(regex_entries)}, "
          f"product_name norm={len(norm_entries)})")


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

    table = catalog.load_table(OliveyoungIceberg.GARBAGE_KEYWORDS_TABLE)
    arrow_table = _to_arrow(table, {
        "match_type": pa.array(match_types, type=pa.string()),
        "keyword":    pa.array(keywords,    type=pa.string()),
        "synced_at":  pa.array(synced_ats,  type=pa.timestamp("us", tz="UTC")),
    })

    table.overwrite(arrow_table)
    print(f"   garbage_keywords sync 완료: {len(keywords)}건 "
          f"(exact={len(config.get('exact', []))}, contains={len(config.get('contains', []))})")


def sync_custom_ingredient_dict(catalog) -> None:
    """
    custom_ingredient_dict.json → Iceberg custom_ingredient_dict 테이블 overwrite

    action:
        'add'      → KCIA 사전에 없는 경우에만 추가 (CoSING/구KCIA 성분, 동의어)
        'override' → KCIA 사전에 있어도 강제 덮어쓰기 (구명칭 충돌 수정)
    """
    entries = _load_json(DataPath.CUSTOM_INGREDIENT_DICT_JSON)

    synced_at = datetime.now(timezone.utc)

    raws      = [e["raw"]                   for e in entries]
    standards = [e.get("standard")          for e in entries]
    actions   = [e["action"]               for e in entries]
    reasons   = [e.get("reason")           for e in entries]
    synced_ats = [synced_at] * len(entries)

    add_count      = sum(1 for a in actions if a == "add")
    override_count = sum(1 for a in actions if a == "override")

    table = catalog.load_table(OliveyoungIceberg.CUSTOM_INGREDIENT_DICT_TABLE)
    arrow_table = _to_arrow(table, {
        "raw":       pa.array(raws,       type=pa.string()),
        "standard":  pa.array(standards,  type=pa.string()),
        "action":    pa.array(actions,    type=pa.string()),
        "reason":    pa.array(reasons,    type=pa.string()),
        "synced_at": pa.array(synced_ats, type=pa.timestamp("us", tz="UTC")),
    })

    table.overwrite(arrow_table)
    print(f"   custom_ingredient_dict sync 완료: {len(entries)}건 "
          f"(add={add_count}, override={override_count})")


# ==========================================
# 실행 진입점
# ==========================================

if __name__ == "__main__":
    print("=== Reference Data sync ===\n")

    catalog = OliveyoungIceberg.get_catalog()

    print(f"[typo] {OliveyoungIceberg.TYPO_MAP_TABLE}")
    sync_typo_map(catalog)

    print(f"\n[garbage] {OliveyoungIceberg.GARBAGE_KEYWORDS_TABLE}")
    sync_garbage_keywords(catalog)

    print(f"\n[custom_ingredient] {OliveyoungIceberg.CUSTOM_INGREDIENT_DICT_TABLE}")
    sync_custom_ingredient_dict(catalog)

    print("\n=== 완료 ===")
