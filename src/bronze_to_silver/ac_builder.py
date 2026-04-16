"""
KCIA 성분 사전 빌드 및 Aho-Corasick 오토마타 모듈

담당:
    - KCIA CSV → 매핑 딕셔너리 생성
    - 유의어/오타 사전 JSON 로드 (typo_map, typo_map_regex)
    - Aho-Corasick 오토마타 빌드 및 탐색
"""

import os
import json
import pandas as pd
import ahocorasick

from config.settings import Iceberg


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


def generate_kcia_mapping_dict(csv_path: str) -> dict:
    """
    KCIA 원본 CSV를 읽어 표준 매핑 딕셔너리를 생성합니다.

    로컬 경로와 S3 경로(s3://) 모두 지원합니다.
    S3 경로의 경우 os.path.exists() 체크를 건너뜁니다.

    매핑 규칙:
        - 표준명, 공백제거 표준명 → 표준명
        - 구이명(old_name_ko, 쉼표 구분) 및 공백제거 버전 → 표준명
        - 쉼표는 _C_ 로 마스킹 (Aho-Corasick 탐색 충돌 방지)

    Args:
        csv_path: KCIA CSV 경로 (로컬 또는 S3)

    Returns:
        dict: {검색키: 표준명칭}

    Raises:
        FileNotFoundError: 로컬 경로인데 파일이 존재하지 않을 때
    """
    if not csv_path.startswith("s3://") and not os.path.exists(csv_path):
        raise FileNotFoundError(f"KCIA CSV 파일을 찾을 수 없습니다: {csv_path}")

    df = pd.read_csv(csv_path)

    if 'std_name_ko' not in df.columns:
        raise ValueError(
            f"CSV에 필수 컬럼 'std_name_ko'가 없습니다. "
            f"실제 컬럼: {list(df.columns)}"
        )

    mapping = {}

    for _, row in df.iterrows():
        if pd.isna(row.get('std_name_ko')):
            continue
        std_name = str(row['std_name_ko']).strip()

        # 표준명 등록
        _kcia_add(mapping, std_name, std_name)

        # 구이명 등록
        old_name = str(row['old_name_ko']).strip()
        _kcia_add(mapping, old_name, std_name)

    return mapping
 
 
def load_kcia_mapping_dict(
    csv_path: str,
    json_cache_path: str,
) -> dict:
    """
    CSV가 있으면 CSV에서 변환해서 사용하고,
    CSV에 문제가 있으면 JSON 폴백으로 로드합니다.
 
    우선순위:
        1. CSV → generate_kcia_mapping_dict() 로 변환 (성공 시 JSON 캐시도 갱신)
        2. CSV 실패 → JSON 폴백 (기존 캐시 사용)
        3. 둘 다 없으면 RuntimeError
 
    Args:
        csv_path:        KCIA 원본 CSV 경로
        json_cache_path: 폴백 및 캐시 저장 경로
 
    Returns:
        dict: KCIA 매핑 딕셔너리
 
    Raises:
        RuntimeError: CSV와 JSON 모두 사용 불가능한 경우
    """
    # 1. CSV 시도
    is_s3 = csv_path.startswith("s3://")
    csv_exists = is_s3 or os.path.exists(csv_path)

    if csv_exists:
        try:
            print(f"   KCIA 사전 생성 중 (CSV: {csv_path}) ...")
            mapping = generate_kcia_mapping_dict(csv_path)
            # 성공 시 JSON 캐시 갱신
            os.makedirs(os.path.dirname(json_cache_path), exist_ok=True)
            with open(json_cache_path, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, ensure_ascii=False, indent=2)
            print(f"   변환 완료: {len(mapping)}개 키워드 (캐시 갱신: {json_cache_path})")
            return mapping
        except Exception as e:
            print(f"   [WARN] CSV 변환 실패 ({e}) → JSON 폴백 시도")
    else:
        print(f"   [WARN] CSV 없음 ({csv_path}) → JSON 폴백 시도")
 
    # 2. JSON 폴백
    if os.path.exists(json_cache_path):
        print(f"   KCIA 사전 JSON 폴백 로드: {json_cache_path}")
        with open(json_cache_path, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        print(f"   로드 완료: {len(mapping)}개 키워드")
        return mapping
 
    # 3. 둘 다 없음
    raise RuntimeError(
        f"KCIA 사전을 로드할 수 없습니다.\n"
        f"  CSV : {csv_path}\n"
        f"  JSON: {json_cache_path}\n"
        f"둘 중 하나가 반드시 존재해야 합니다."
    )


# ==========================================
# 2. Reference Data Iceberg 로드
# ==========================================


# ==========================================
# 3. Reference Data Iceberg 로드
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
    table = catalog.load_table(Iceberg.TYPO_MAP_TABLE)
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
    table = catalog.load_table(Iceberg.TYPO_MAP_TABLE)
    df = table.scan().to_arrow().to_pandas()

    if "apply_to" not in df.columns:
        print("   product_name_norm: apply_to 컬럼 없음 — 빈 목록 반환")
        return []

    df = df[df["apply_to"] == "product_name"][["raw", "fix", "match_type"]]
    norm_list = df.to_dict("records")

    print(f"   product_name_norm 로드: {len(norm_list)}개 항목")
    return norm_list



def load_garbage_config_from_iceberg(catalog) -> dict:
    """
    Iceberg garbage_keywords 테이블에서 가비지 필터링 설정을 로드합니다.

    Returns:
        dict: {"exact": [...], "contains": [...]}
    """
    table = catalog.load_table(Iceberg.GARBAGE_KEYWORDS_TABLE)
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