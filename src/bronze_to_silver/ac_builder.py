"""
KCIA 성분 사전 빌드 및 Aho-Corasick 오토마타 모듈

담당:
    - KCIA CSV → 매핑 딕셔너리 생성
    - 유의어/오타 JSON 로드
    - Aho-Corasick 오토마타 빌드 및 탐색
"""

import os
import json
import pandas as pd
import ahocorasick


# ==========================================
# 1. KCIA 매핑 딕셔너리 생성
# ==========================================

def generate_kcia_mapping_dict(csv_path: str) -> dict:
    """
    KCIA 원본 CSV를 읽어 표준 매핑 딕셔너리를 생성합니다.
 
    매핑 규칙:
        - 표준명, 공백제거 표준명 → 표준명
        - 구이명(old_name_ko, 쉼표 구분) 및 공백제거 버전 → 표준명
        - 쉼표는 _C_ 로 마스킹 (Aho-Corasick 탐색 충돌 방지)
 
    Args:
        csv_path: kcia_ingredients_source.csv 경로
 
    Returns:
        dict: {검색키: 표준명칭}
 
    Raises:
        FileNotFoundError: CSV 파일이 존재하지 않을 때
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"KCIA CSV 파일을 찾을 수 없습니다: {csv_path}")
 
    df = pd.read_csv(csv_path)
 
    if 'std_name_ko' not in df.columns:
        raise ValueError(
            f"CSV에 필수 컬럼 'std_name_ko'가 없습니다. "
            f"실제 컬럼: {list(df.columns)}"
        )
 
    mapping = {}
 
    def _add(name: str, std_name: str):
        if not name or pd.isna(name):
            return
        name = str(name).strip()
        if not name or name == 'nan':
            return
        masked = name.replace(",", "_C_")
        mapping[masked] = std_name
        mapping[masked.replace(" ", "")] = std_name
 
    for _, row in df.iterrows():
        if pd.isna(row.get('std_name_ko')):
            continue
        std_name = str(row['std_name_ko']).strip()
 
        # 표준명 등록
        _add(std_name, std_name)

        # 구이명 등록
        old_name = str(row['old_name_ko']).strip()
        _add(old_name, std_name)
 
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
    if os.path.exists(csv_path):
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
# 2. 유의어/오타 사전 로드
# ==========================================

def load_synonym_dict(json_path: str) -> dict:
    """
    유의어/오타 매핑 JSON을 로드합니다.
    파일이 없으면 빈 딕셔너리를 반환합니다.

    Args:
        json_path: synonym_mapping.json (또는 typo_map.json) 경로

    Returns:
        dict: {오타/유의어: 표준표현}
    """
    if not os.path.exists(json_path):
        print(f"   유의어 사전 없음 (건너뜀): {json_path}")
        return {}
    with open(json_path, 'r', encoding='utf-8') as f:
        synonym_dict = json.load(f)
    print(f"   유의어 사전 로드: {len(synonym_dict)}개 항목")
    return synonym_dict


# ==========================================
# 3. Aho-Corasick 오토마타 빌드 및 탐색
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