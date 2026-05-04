"""
KCIA 성분 사전 빌드 및 Aho-Corasick 오토마타 모듈

담당:
    - KCIA CSV → 매핑 딕셔너리 생성
    - 유의어/오타 사전 JSON 로드 (typo_map, typo_map_regex)
    - Aho-Corasick 오토마타 빌드 및 탐색
"""

import pandas as pd
import ahocorasick

from config.settings import OliveyoungIceberg, INCIIceberg


# ==========================================
# 1. KCIA 매핑 딕셔너리 생성
# ==========================================

def _kcia_add(mapping: dict, name: str, std_name: str) -> None:
    """name을 마스킹/공백제거하여 mapping에 std_name으로 등록합니다."""
    if not name or pd.isna(name):
        return
    name = str(name).strip()
    if not name or name == 'nan':
        return
    masked = name.replace(",", "_C_")
    mapping[masked] = std_name
    mapping[masked.replace(" ", "")] = std_name


def generate_kcia_mapping_dict(inci_catalog) -> dict:
    """
    Iceberg silver_kcia_cosing_graphrag_current 테이블을 읽어 표준 매핑 딕셔너리를 생성합니다.

    매핑 규칙:
        - 표준명, 공백제거 표준명 → 표준명
        - 구이명(old_name_ko) 및 공백제거 버전 → 표준명
        - 쉼표는 _C_ 로 마스킹 (Aho-Corasick 탐색 충돌 방지)

    Args:
        inci_catalog: INCIIceberg.get_catalog()로 생성한 Catalog 인스턴스

    Returns:
        dict: {검색키: 표준명칭}
    """
    table = inci_catalog.load_table(INCIIceberg.SILVER_GRAPHRAG_CURRENT_TABLE)
    df = table.scan(selected_fields=("std_name_ko", "old_name_ko")).to_arrow().to_pandas()

    mapping = {}
    for _, row in df.iterrows():
        if pd.isna(row.get("std_name_ko")):
            continue
        std_name = str(row["std_name_ko"]).strip()
        _kcia_add(mapping, std_name, std_name)
        _kcia_add(mapping, row.get("old_name_ko"), std_name)

    return mapping
 
 

# ==========================================
# 2. Reference Data Iceberg 로드
# ==========================================

def load_typo_maps_from_iceberg(catalog) -> tuple[list[dict], list[dict]]:
    """
    Iceberg typo_map 테이블에서 성분명 오타/유의어 사전을 로드합니다.
    apply_to='ingredient' 행만 대상으로 합니다.

    정렬 기준: raw 길이 내림차순 (긴 raw부터 치환해야 부분집합 오염 방지)

    Returns:
        (typo_list, typo_regex_list)
            typo_list:       match_type='simple'         → list[{"raw", "fix"}]
            typo_regex_list: match_type='regex_boundary' → list[{"raw", "fix"}]
    """
    table = catalog.load_table(OliveyoungIceberg.TYPO_MAP_TABLE)
    df = table.scan().to_arrow().to_pandas()

    # apply_to 컬럼이 있으면 ingredient 행만, 없으면 전체(하위 호환)
    if "apply_to" in df.columns:
        df = df[df["apply_to"].isin(["ingredient", None]) | df["apply_to"].isna()]

    df = (
        df.assign(_raw_len=df["raw"].str.len())
          .sort_values("_raw_len", ascending=False)
          .drop(columns=["_raw_len", "synced_at", "apply_to"], errors="ignore")
    )

    typo_list       = df[df["match_type"] == "simple"][["raw", "fix"]].to_dict("records")
    typo_regex_list = df[df["match_type"] == "regex_boundary"][["raw", "fix"]].to_dict("records")

    print(f"   typo_map 로드: simple={len(typo_list)}, regex_boundary={len(typo_regex_list)}개 항목")
    return typo_list, typo_regex_list


def load_product_name_norms_from_iceberg(catalog) -> list[dict]:
    """
    Iceberg typo_map 테이블에서 제품명 표기 정규화 규칙을 로드합니다.
    apply_to='product_name' 행만 대상으로 합니다.

    Returns:
        list[{"raw", "fix", "match_type"}]
            match_type='regex'  → re.compile(raw).sub(fix, text)
            match_type='simple' → text.replace(raw, fix)
    """
    table = catalog.load_table(OliveyoungIceberg.TYPO_MAP_TABLE)
    df = table.scan().to_arrow().to_pandas()

    if "apply_to" not in df.columns:
        print("   product_name_norm: apply_to 컬럼 없음 — 빈 목록 반환")
        return []

    df = df[df["apply_to"] == "product_name"][["raw", "fix", "match_type"]]
    norm_list = df.to_dict("records")

    print(f"   product_name_norm 로드: {len(norm_list)}개 항목")
    return norm_list



def load_custom_ingredient_dict_from_iceberg(catalog) -> list[dict]:
    """
    Iceberg custom_ingredient_dict 테이블에서 커스텀 성분 사전을 로드합니다.

    Returns:
        list[{"raw", "standard", "action"}]
            action='add'      → KCIA에 없는 경우에만 추가
            action='override' → KCIA에 있어도 강제 덮어쓰기
    """
    table = catalog.load_table(OliveyoungIceberg.CUSTOM_INGREDIENT_DICT_TABLE)
    df = table.scan().to_arrow().to_pandas()

    entries = df[["raw", "standard", "action"]].to_dict("records")

    add_count      = sum(1 for e in entries if e["action"] == "add")
    override_count = sum(1 for e in entries if e["action"] == "override")
    print(f"   custom_ingredient_dict 로드: {len(entries)}건 "
          f"(add={add_count}, override={override_count})")
    return entries


def apply_custom_ingredient_dict(mapping: dict, custom_entries: list[dict]) -> dict:
    """
    KCIA 매핑 딕셔너리에 커스텀 성분 사전을 적용합니다.

    Aho-Corasick 빌드 전에 호출해야 합니다.

    Args:
        mapping:       generate_kcia_mapping_dict()로 생성한 딕셔너리
        custom_entries: load_custom_ingredient_dict_from_iceberg()로 로드한 목록

    Returns:
        dict: 커스텀 항목이 반영된 매핑 딕셔너리
    """
    for entry in custom_entries:
        raw      = str(entry["raw"]).strip()
        standard = entry["standard"]
        action   = entry["action"]

        if not raw:
            continue

        masked          = raw.replace(",", "_C_")
        masked_no_space = masked.replace(" ", "")

        if action == "add":
            if masked not in mapping:
                mapping[masked] = standard
            if masked_no_space not in mapping:
                mapping[masked_no_space] = standard
        elif action == "override":
            mapping[masked]          = standard
            mapping[masked_no_space] = standard

    return mapping


def load_garbage_config_from_iceberg(catalog) -> dict:
    """
    Iceberg garbage_keywords 테이블에서 가비지 필터링 설정을 로드합니다.

    Returns:
        dict: {"exact": [...], "contains": [...]}
    """
    table = catalog.load_table(OliveyoungIceberg.GARBAGE_KEYWORDS_TABLE)
    df = table.scan().to_arrow().to_pandas()

    config = {
        "exact":    df[df["match_type"] == "exact"]["keyword"].tolist(),
        "contains": df[df["match_type"] == "contains"]["keyword"].tolist(),
    }

    print(f"   garbage_keywords 로드: exact={len(config['exact'])}, contains={len(config['contains'])}")
    return config


# ==========================================
# 4. Aho-Corasick 오토마타 빌드 및 탐색
# ==========================================

def build_ahocorasick(mapping_dict: dict) -> ahocorasick.Automaton:
    """
    매핑 딕셔너리로 Aho-Corasick 오토마타를 빌드합니다.

    Args:
        mapping_dict: {검색키: 표준명칭}

    Returns:
        ahocorasick.Automaton
    """
    A = ahocorasick.Automaton()
    for search_key, std_name in mapping_dict.items():
        A.add_word(search_key, (search_key, std_name))
    A.make_automaton()
    return A


def search_with_ac(
    processed_text: str,
    automaton: ahocorasick.Automaton
) -> tuple[list[str], str]:
    """
    공백 제거 + _C_ 마스킹된 텍스트에서 성분을 탐색하고 잔여물을 반환합니다.

    동작:
        1. 오토마타 순회하며 전체 매칭 수집
        2. 시작 인덱스 순, 동일 시작이면 길이 내림차순으로 정렬
        3. Greedy Filter: 겹치는 구간 중 먼저 나온 최장 매칭만 선택
        4. 매칭되지 않은 나머지 텍스트를 residual로 반환

    Args:
        processed_text: 공백 제거 + _C_ 마스킹된 성분 문자열
        automaton:      빌드된 Aho-Corasick 오토마타

    Returns:
        (matches, residual):
            matches  - 표준명칭 리스트 (매칭 순서)
            residual - 매칭되지 않은 잔여 텍스트
    """
    if not processed_text:
        return [], ""

    matches = []
    for end_idx, (matched_key, std_name) in automaton.iter(processed_text):
        start_idx = end_idx - len(matched_key) + 1
        matches.append((start_idx, end_idx, std_name))

    if not matches:
        return [], processed_text

    # 시작 인덱스 오름차순, 동일 시작이면 길이 내림차순
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))

    final_ingredients = []
    matched_intervals = []
    last_end = -1

    for start, end, std_name in matches:
        if start > last_end:  # 겹치지 않을 때만 채택
            final_ingredients.append(std_name)
            matched_intervals.append((start, end))
            last_end = end

    # 매칭 구간 제외한 잔여 텍스트 추출
    residual = ""
    curr_pos = 0
    for start, end in matched_intervals:
        residual += processed_text[curr_pos:start]
        curr_pos = end + 1
    residual += processed_text[curr_pos:]

    return final_ingredients, residual