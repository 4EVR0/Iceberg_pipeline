"""
올리브영 크롤링 데이터 전처리 파이프라인 모듈

담당:
    - 정규표현식 상수 정의
    - 성분 문자열 정제 (무효 문구 제거, 번들 탐지, 괄호 제거 등)
    - 제품명 클리닝
    - 중복 제거
    - 성분 매칭 및 silver/error 라우팅
"""

import re
import uuid
from datetime import datetime, timezone

import pandas as pd
import ahocorasick

from src.bronze_to_silver.ac_builder import search_with_ac
from models.pipeline_models import ErrorRecord


# ==========================================
# 정규표현식 상수
# ==========================================

# 무효 데이터 및 안내 문구 제거
REGEX_PREFIX_ALL = re.compile(r'^전성분(?:명)?\s*:?\s*')
REGEX_LEGEND = re.compile(
    r'(?:(?:※|\*|\+)\s*(?:표시\s*:|자연\s*유래|식물\s*유래|유기농|ILN\d+)|'
    r'(?:※|\*|\+)?\s*(?:제공된\s*성분은|ILN\d+\s*성분\s*목록은))'
    r'.*?(?=\[|<|(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]|$)'
)
REGEX_ILN = re.compile(r'(?i)[<\[]?ILN\d+[>\]]?')
REGEX_LEGEND_WITH_BRACKET = re.compile(r'\([^)]*[*※+][^)]*\)')
REGEX_SYMBOLS = re.compile(r'[*※+]')

# 무첨가성분 안내 문구 제거 (말미의 "무첨가 성분: ..." 전체 삭제)
# 예) "토코페롤, 소듐파이테이트 * 무첨가 성분: 페녹시에탄올, PEG ..."
REGEX_NO_INGREDIENT = re.compile(
    r'[*※]?\s*무첨가\s*성분\s*:.*$',
    re.IGNORECASE | re.DOTALL
)

# 이종 결합 번들 탐지
REGEX_BUNDLE = re.compile(
    r'(?:<[^>]+>|'                                                          # <HTML태그> 형식
    r'■\s*[^:■]+:\s*|'                                                      # ■ 섹션명: 형식
    r'[가-힣a-zA-Z0-9]+\s*-\s*\[전성분|'                                     # 한글명-[전성분 형식
    r'\[(?![Ii][Ll][Nn])[^\]]+\]|'                                         # [대괄호] (ILN 제외, 본품/구성품 포함)
    r'(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]+|'                                  # 숫자) 형식
    r'[가-힣a-zA-Z0-9\s]+(?:볼|앰플|세럼|크림|토너|로션)\s*[):]\s*(?=[가-힣])'  # 제품유형 구분자
    r')'
)
REGEX_BONPUM_MULTI = re.compile(r'\[본품\]')
REGEX_PRODUCT_OPTION_BUNDLE = re.compile(
    r'\d+\s*[종가지]\s*(?:(?:택|중|중\s*\d+)\s*(?:1|일|택|선택))?'
    r'|\b(?:택|선택)\s*\d+\b'
)

# 성분명 농도/이명 괄호 제거
REGEX_BRACKET = re.compile(
    r'\([^)]*('
    r'ppm|PPM|ppb|PPB|%|mg|mG|G|g|ml|mL|IU|'
    r'NON\s*GMO|CI\s*\d+|BHA|AHA|'
    r'산\s*|전성분|전성분명|성분|유래|추출물|아쿠아'
    r'|[\d.]+\s*(?:%|ppm|ppb|mg|ml|g)?$'
    r').*?\)'
)

# 제품명 전처리
REGEX_PRODUCT_BRACKET = re.compile(r'\[.*?\]|\(.*?\)')
REGEX_PRODUCT_VOLUME_ANCHOR = re.compile(
    r'('
    r'\d+(?:\.\d+)?\s*[+xX*]\s*\d+.*|'                                           # 3+1, 50+30ml 등 수량/용량 결합
    r'\d+(?:\.\d+)?\s*(?:ml|㎖|l|ℓ|g|매|호|ea|개입|입|p|종)(?=[^a-zA-Z가-힣]|$).*|'  # 150ml, 100g 등
    r'\b\d+(?:\.\d+)?$'                                                           # 문장 끝에 혼자 남은 숫자
    r').*',
    re.IGNORECASE
)
REGEX_PRODUCT_MARKETING = re.compile(
    r'기획|증정|단독|1\+1|본품|추가|대용량|세트|듀오|트리플|한정|리필팩|리필|단품|구성'
    r'|트래블\s*키트|캡슐\s*키트'
    r'|더블\s*(기획|세트|구성|\d+입|\d+\s*(ml|g|㎖))',
    re.IGNORECASE
)
REGEX_PRODUCT_TAIL_SYMBOLS = re.compile(r'[\s+*/\-.,]+$|(?<=\s)[+*/\-,.]+(?=\s|$)')
REGEX_MULTI_SPACE = re.compile(r'\s+')

# 성분명 내 쉼표 마스킹 (영숫자 사이의 쉼표)
REGEX_COMMA_MASK = re.compile(r'(?<=[A-Za-z0-9]),(?=[A-Za-z0-9])')

# 성분 문자열 전처리 (줄바꿈/탭 제거, 구분자 통일)
REGEX_WHITESPACE_CTRL = re.compile(r'[\r\n\t]')
REGEX_ALT_SEPARATOR   = re.compile(r'[@|]')

# typo_map_regex 공통 경계 패턴 (성분 단독 보장)
_TYPO_RE_BOUNDARY = r'(?<![가-힣a-zA-Z0-9\-./]){raw}(?![가-힣a-zA-Z0-9\-./])'


# ==========================================
# 카테고리 lookup 헬퍼
# ==========================================

def build_category_lookup(category_df: pd.DataFrame) -> dict:
    """
    category_master DataFrame으로부터 (main_category, sub_category) → category_id
    lookup 딕셔너리를 생성합니다.

    category_master의 category_id 생성 규칙:
        f"{main_category}_{sub_category}".replace(" ", "").replace("/", "-")

    Args:
        category_df: oliveyoung_category_master 테이블
                     컬럼: category_id, main_category, sub_category

    Returns:
        dict: {(main_category, sub_category): category_id}
    """
    return {
        (m, s): c
        for m, s, c in zip(
            category_df["main_category"],
            category_df["sub_category"],
            category_df["category_id"],
        )
    }


def resolve_category_id(
    main_category: str,
    sub_category: str,
    lookup: dict
) -> str:
    """
    (main_category, sub_category) 쌍으로 category_id를 반환합니다.
    매칭 실패 시 None을 반환합니다.

    category_master의 sub_category 값이 슬래시(/)를 포함할 수 있으므로
    raw 데이터의 sub_category와 정확히 일치 여부를 먼저 확인하고,
    실패하면 슬래시를 하이픈으로 치환한 값으로 재시도합니다.

    예) raw: "오일/밤"  →  category_master: "오일/밤" (직접 매칭)
        raw: "클렌징폼/젤" →  category_master: "클렌징폼/젤" (직접 매칭)
    """
    # 1차: 원형 그대로 매칭
    category_id = lookup.get((main_category, sub_category))
    if category_id:
        return category_id

    # 2차: sub_category의 슬래시를 하이픈으로 치환 후 재시도
    #       (크롤러와 category_master 생성 로직 간 표기 차이 대비)
    sub_normalized = sub_category.replace("/", "-")
    return lookup.get((main_category, sub_normalized))


# ==========================================
# 내부 헬퍼
# ==========================================


# 올리브영 도메인 네임스페이스 — 다른 쇼핑몰 데이터 추가 시 별도 네임스페이스로 격리
_OLIVEYOUNG_NS = uuid.uuid5(uuid.NAMESPACE_DNS, "oliveyoung.co.kr")


def _make_product_id(brand: str, clean_name: str) -> str:
    """
    브랜드명 + 정제된 제품명 기반 UUID v5를 product_id로 반환합니다.
 
    UUID v5(SHA-1 + 네임스페이스)를 사용하여:
        - 동일 제품 재처리 시 항상 동일한 ID 생성 (결정적)
        - RFC 4122 표준 포맷으로 타 시스템 연동 호환성 확보
        - 네임스페이스로 도메인(올리브영) 격리
 
    Args:
        brand:      제품 브랜드명
        clean_name: Step 4에서 정제된 제품명
 
    Returns:
        str: UUID v5 문자열 (예: 'a1b2c3d4-e5f6-5789-abcd-ef0123456789')
    """
    return str(uuid.uuid5(_OLIVEYOUNG_NS, f"{brand}||{clean_name}"))
 
 
def _make_error(
    product_id:              str,
    category_id:             str | None,
    brand:                   str,
    product_name_raw:        str,
    product_name:            str,
    raw_text:                str,
    url:                     str,
    crawled_at,
    error_type:              str,
    residual_text:           str,
) -> ErrorRecord:
    """에러 레코드를 생성합니다."""
    return ErrorRecord(
        product_id              = product_id,
        category_id             = category_id,
        product_brand           = brand,
        product_name_raw        = product_name_raw,
        product_name            = product_name,
        product_ingredients_raw = raw_text,
        product_url             = url,
        crawled_at              = crawled_at,
        error_type              = error_type,
        residual_text           = residual_text,
    )


def _is_blank(v: str) -> bool:
    """빈 문자열 또는 'nan' 문자열이면 True를 반환합니다."""
    return not v or v.strip() in ('', 'nan')


def _is_garbage_name(name: str, cfg: dict) -> bool:
    """
    garbage_keywords.json 설정을 기반으로 제품명이 크롤링 오류 텍스트인지 판별합니다.

    판별 순서:
        1. exact   - 완전 일치
        2. contains - 포함 여부

    Args:
        name: 판별할 제품명 (product_name_raw)
        cfg:  garbage_keywords.json을 로드한 dict (None이면 항상 False)

    Returns:
        True면 garbage (INVALID_METADATA_REJECTED 처리 대상)
    """
    if not cfg:
        return False
    if name in cfg.get("exact", []):
        return True
    for kw in cfg.get("contains", []):
        if kw in name:
            return True
    return False


def _apply_typo_maps(
    text: str,
    typo_regex_list: list[dict],
    typo_list: list[dict],
) -> str:
    """
    typo_map_regex.json(정규식 기반) → typo_map.json(단순 치환) 순서로 오타를 보정합니다.

    적용 순서:
        1. typo_map_regex: 부분집합 오염 위험이 있는 케이스. raw 길이 내림차순으로
           미리 정렬된 상태로 전달받으며, 경계 패턴으로 단독 성분만 치환합니다.
        2. typo_map: 단순 문자열 치환. raw 길이 내림차순으로 미리 정렬된 상태로 전달받습니다.

    Args:
        text:             치환 대상 텍스트
        typo_regex_list:  typo_map_regex.json 로드 결과 (list[{"raw", "fix", "pattern"}])
        typo_list:        typo_map.json 로드 결과 (list[{"raw", "fix"}])

    Returns:
        str: 오타 보정된 텍스트
    """
    # 1. 정규식 기반 치환 (우선 적용, 긴 raw부터)
    for entry in typo_regex_list:
        if entry["raw"] in text:
            pattern = _TYPO_RE_BOUNDARY.format(raw=re.escape(entry["raw"]))  # ← format 추가
            text = re.sub(pattern, entry["fix"], text)

    # 2. 단순 문자열 치환 (긴 raw부터)
    for entry in typo_list:
        if entry["raw"] in text:
            text = text.replace(entry["raw"], entry["fix"])

    return text


# ==========================================
# 전처리 파이프라인 (내부 단계 함수)
# ==========================================

def _clean_rows(
    df: pd.DataFrame,
    category_lookup: dict,
    typo_list: list[dict],
    typo_regex_list: list[dict],
    garbage_config: dict,
) -> tuple[list[dict], list[dict]]:
    """
    Step 2~9: 행별 정제를 수행하여 interim_list와 error_records를 반환합니다.

    - Step 2: 누락 필드 검사 → INCOMPLETE_DATA_REJECTED
    - Step 3: 제품명 기반 필터링 (옵션 번들, garbage)
    - Step 4: 제품명 클리닝 + product_id 생성
    - Step 5: 특수기호 제거 및 구분자 치환
    - Step 6: 오타 사전 치환
    - Step 7: 무효 문구 소거
    - Step 8: 이종 결합 번들 탐지
    - Step 9: 성분명 농도/이명 괄호 제거
    """
    interim_list  = []
    error_records = []

    for _, row in df.iterrows():
        raw_text         = str(row.get('ingredients', ''))
        product_name_raw = str(row.get('name', ''))
        brand            = str(row.get('brand', ''))
        url              = str(row.get('url', ''))
        main_category    = str(row.get('main_category', ''))
        sub_category     = str(row.get('sub_category', ''))

        category_id = resolve_category_id(main_category, sub_category, category_lookup)

        crawled_at_raw = row.get('crawled_at', None)
        try:
            crawled_at = pd.Timestamp(crawled_at_raw, tz="UTC")
        except Exception:
            crawled_at = pd.NaT

        try:
            rating = float(row.get('rating', 0.0))
        except (ValueError, TypeError):
            rating = 0.0

        try:
            review_count = int(float(row.get('review_count', 0)))
        except (ValueError, TypeError):
            review_count = 0

        review_stats = row.get('review_stats', {})

        # [Step 2] 누락 필드 검사
        missing_fields = []
        if _is_blank(raw_text):         missing_fields.append('ingredients')
        if _is_blank(product_name_raw): missing_fields.append('name')
        if _is_blank(brand):            missing_fields.append('brand')
        if _is_blank(url):              missing_fields.append('url')
        if pd.isnull(crawled_at):       missing_fields.append('crawled_at')
        if _is_blank(main_category):    missing_fields.append('main_category')
        if _is_blank(sub_category):     missing_fields.append('sub_category')

        if missing_fields:
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, category_id, brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'INCOMPLETE_DATA_REJECTED',
                f"Missing fields: {', '.join(missing_fields)}",
            ))
            continue

        # [Step 3a] 옵션 번들 필터링
        if REGEX_PRODUCT_OPTION_BUNDLE.search(product_name_raw):
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, category_id, brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'OPTION_BUNDLE_REJECTED',
                'Multi-option product (n-종) detected in name',
            ))
            continue

        # [Step 3b] garbage 제품명 필터링
        if _is_garbage_name(product_name_raw, garbage_config):
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, category_id, brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'INVALID_METADATA_REJECTED',
                f"Garbage name detected: {product_name_raw!r}",
            ))
            continue

        # [Step 4] 제품명 클리닝 + product_id 생성
        product_name = REGEX_PRODUCT_BRACKET.sub(' ', product_name_raw)
        product_name = REGEX_PRODUCT_VOLUME_ANCHOR.sub('', product_name)
        product_name = REGEX_PRODUCT_MARKETING.sub('', product_name)
        product_name = REGEX_PRODUCT_TAIL_SYMBOLS.sub('', product_name)
        clean_product_name = REGEX_MULTI_SPACE.sub(' ', product_name).strip()
        product_id = _make_product_id(brand, clean_product_name)

        text = raw_text

        # [Step 5] 특수기호 제거 및 구분자 치환
        text = REGEX_WHITESPACE_CTRL.sub('', text)
        text = REGEX_ALT_SEPARATOR.sub(',', text)

        # [Step 6] 오타 사전 치환
        text = _apply_typo_maps(text, typo_regex_list, typo_list)

        # [Step 7] 무효 문구 소거
        text = REGEX_PREFIX_ALL.sub('', text)
        text = REGEX_NO_INGREDIENT.sub('', text)
        text = REGEX_LEGEND.sub('', text)
        text = REGEX_ILN.sub('', text)
        text = REGEX_LEGEND_WITH_BRACKET.sub('', text)
        text = REGEX_SYMBOLS.sub('', text)

        # [Step 8] 이종 결합 번들 탐지
        if len(REGEX_BONPUM_MULTI.findall(text)) >= 2:
            error_records.append(_make_error(
                product_id, category_id, brand, product_name_raw, clean_product_name,
                raw_text, url, crawled_at,
                'HETEROGENEOUS_BUNDLE_REJECTED', text,
            ))
            continue

        bundle_count = len(REGEX_BUNDLE.findall(text))
        if bundle_count >= 2:
            error_records.append(_make_error(
                product_id, category_id, brand, product_name_raw, clean_product_name,
                raw_text, url, crawled_at,
                'HETEROGENEOUS_BUNDLE_REJECTED', text,
            ))
            continue
        elif bundle_count == 1:
            text = REGEX_BUNDLE.sub('', text)

        # [Step 9] 성분명 농도/이명 괄호 제거
        text = REGEX_BRACKET.sub('', text)

        interim_list.append({
            'product_id':              product_id,
            'category_id':             category_id,
            'product_brand':           brand,
            'product_name':            clean_product_name,
            'product_name_raw':        product_name_raw,
            'cleaned_text_str':        text,
            'product_ingredients_raw': raw_text,
            'rating':                  rating,
            'review_count':            review_count,
            'review_stats':            review_stats,
            'product_url':             url,
            'crawled_at':              crawled_at,
        })

    return interim_list, error_records


def _dedup_interim(interim_list: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    """
    Step 10: 브랜드 + 정규화 이름 기준 중복을 제거합니다.
    성분 문자열이 짧은 쪽을 유지하고, 나머지는 DUPLICATE_PRODUCT_REJECTED로 반환합니다.
    """
    interim_df = pd.DataFrame(interim_list)
    interim_df['text_len']  = interim_df['cleaned_text_str'].apply(len)
    interim_df['name_norm'] = interim_df['product_name'].str.replace(" ", "", regex=False)
    interim_df = interim_df.sort_values('text_len', ascending=True)

    duplicate_mask = interim_df.duplicated(subset=['product_brand', 'name_norm'], keep='first')

    duplicate_errors = [
        _make_error(
            r['product_id'], r['category_id'],
            r['product_brand'], r['product_name_raw'], r['product_name'],
            r['product_ingredients_raw'], r['product_url'], r['crawled_at'],
            'DUPLICATE_PRODUCT_REJECTED',
            f"Duplicate of {r['product_brand']} | {r['product_name']}",
        )
        for r in interim_df[duplicate_mask].to_dict('records')
    ]

    deduped_df = interim_df[~duplicate_mask]
    return deduped_df, duplicate_errors


def _match_ingredients(
    deduped_df: pd.DataFrame,
    ac_automaton: ahocorasick.Automaton,
) -> tuple[list[dict], list[dict]]:
    """
    Step 11~13: Aho-Corasick 성분 매칭 후 silver / error 레코드를 반환합니다.

    - Step 11: 성분명 내 쉼표 마스킹
    - Step 12: Aho-Corasick 탐색 + 히든 번들 탐지
    - Step 13: silver / error 라우팅
    """
    silver_records = []
    error_records  = []

    for _, row in deduped_df.iterrows():
        text        = row['cleaned_text_str']
        product_id  = row['product_id']
        url         = row['product_url']
        crawled_at  = row['crawled_at']
        category_id = row['category_id']

        # [Step 11] 성분명 내 쉼표 마스킹
        text = REGEX_COMMA_MASK.sub('_C_', text)

        # [Step 12-1] 공백 제거 후 Aho-Corasick 탐색
        if ',' in text:
            processed_text = ",".join(p.replace(" ", "") for p in text.split(','))
        else:
            processed_text = text.replace(" ", "")

        matches, residual = search_with_ac(processed_text, ac_automaton)

        # [Step 12-2] 히든 번들 탐지
        if matches and (matches.count('정제수') >= 2 or matches.count('글리세린') >= 2):
            error_records.append(_make_error(
                product_id, category_id,
                row['product_brand'], row['product_name_raw'], row['product_name'],
                row['product_ingredients_raw'], url, crawled_at,
                'HIDDEN_BUNDLE_REJECTED', 'Duplicate Core Ingredients',
            ))
            continue

        # [Step 12-3] 잔여 텍스트 마스킹 복원
        residual_text = residual.replace("_C_", ",").strip()

        # [Step 13] silver / error 라우팅
        if matches:
            seen = set()
            deduped = [ing for ing in matches if ing not in seen and not seen.add(ing)]

            silver_records.append({
                'product_id':              product_id,
                'category_id':             category_id,
                'product_brand':           row['product_brand'],
                'product_name':            row['product_name'],
                'product_ingredients':     deduped,
                'product_name_raw':        row['product_name_raw'],
                'product_ingredients_raw': row['product_ingredients_raw'],
                'rating':                  row['rating'],
                'review_count':            row['review_count'],
                'review_stats':            row['review_stats'],
                'product_url':             url,
                'crawled_at':              crawled_at,
            })

        if (residual_text
                and len(residual_text.strip()) > 2
                and re.search(r'[가-힣a-zA-Z0-9]', residual_text)):
            error_records.append(_make_error(
                product_id, category_id,
                row['product_brand'], row['product_name_raw'], row['product_name'],
                row['product_ingredients_raw'], url, crawled_at,
                'UNMAPPED_RESIDUAL', residual_text,
            ))

    return silver_records, error_records


# ==========================================
# 전처리 파이프라인 (오케스트레이터)
# ==========================================

def process_pipeline(
    df: pd.DataFrame,
    ac_automaton: ahocorasick.Automaton,
    typo_list: list[dict],
    typo_regex_list: list[dict],
    category_df: pd.DataFrame = None,
    garbage_config: dict = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Bronze raw DataFrame을 받아 silver / error DataFrame으로 전처리합니다.

    처리 흐름:
        Step 1  - category lookup 딕셔너리 빌드
        Step 2~9  → _clean_rows()
        Step 10   → _dedup_interim()
        Step 11~13 → _match_ingredients()

    Args:
        df:               Bronze raw DataFrame
        ac_automaton:     빌드된 Aho-Corasick 오토마타
        typo_list:        typo_map.json 로드 결과 (list[{"raw", "fix"}], 길이 내림차순)
        typo_regex_list:  typo_map_regex.json 로드 결과 (list[{"raw", "fix", "pattern"}], 길이 내림차순)
        category_df:      oliveyoung_category_master DataFrame (None이면 category_id=None)
        garbage_config:   garbage_keywords.json 로드 결과 (None이면 garbage 필터 미적용)

    Returns:
        (silver_df, error_df)
    """
    batch_job  = "oliveyoung_bronze_to_silver_process"
    batch_date = datetime.now(timezone.utc)

    # [Step 1] category lookup 빌드
    category_lookup = build_category_lookup(category_df) if category_df is not None else {}

    # [Step 2~9] 행별 정제
    interim_list, error_records = _clean_rows(
        df, category_lookup, typo_list, typo_regex_list, garbage_config
    )

    if not interim_list:
        return pd.DataFrame(), pd.DataFrame([r.to_dict() for r in error_records])

    # [Step 10] 중복 제거
    deduped_df, duplicate_errors = _dedup_interim(interim_list)
    error_records.extend(duplicate_errors)

    # [Step 11~13] 성분 매칭
    silver_records, match_errors = _match_ingredients(deduped_df, ac_automaton)
    error_records.extend(match_errors)

    silver_df = pd.DataFrame(silver_records)
    error_df  = pd.DataFrame([r.to_dict() for r in error_records])

    for df_ in (silver_df, error_df):
        if not df_.empty:
            df_["batch_job"]  = batch_job
            df_["batch_date"] = pd.Timestamp(batch_date)

    return silver_df, error_df