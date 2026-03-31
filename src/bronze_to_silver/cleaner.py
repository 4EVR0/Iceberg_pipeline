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
import pandas as pd
import ahocorasick


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

# 이종 결합 번들 탐지
REGEX_BUNDLE = re.compile(
    r'(?:<[^>]+>|'
    r'■\s*[^:■]+:\s*|'
    r'[가-힣a-zA-Z0-9]+\s*-\s*\[전성분|'
    r'\[(?![Ii][Ll][Nn])[^\]]+\]|'
    r'(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]+)'
)
REGEX_PRODUCT_OPTION_BUNDLE = re.compile(
    r'\d+\s*[종가지]\s*(?:(?:택|중|중\s*\d+)\s*(?:1|일|택|선택))?'
    r'|\b(?:택|선택)\s*\d+\b'
)

# 성분명 농도/이명 괄호 제거
REGEX_BRACKET = re.compile(
    r'\([^)]*('
    r'ppm|ppb|%|mg|mG|G|g|ml|mL|'
    r'NON\s*GMO|CI\s*\d+|BHA|AHA|'
    r'산\s*|성분|온천수|유래|추출물'
    r'|[\d.]+\s*'
    r').*?\)'
)

# 제품명 전처리
REGEX_PRODUCT_BRACKET = re.compile(r'\[.*?\]|\(.*?\)')
REGEX_PRODUCT_VOLUME_ANCHOR = re.compile(
    r'('
    r'\d+(?:\.\d+)?\s*[+xX*]\s*\d+.*|'  # 3+1, 50+30ml, 1+1기획 등 (수량/용량 결합)
    r'\d+(?:\.\d+)?\s*(?:ml|㎖|l|ℓ|g|매|호|ea|개입|입|p|종).*|' # 150ml, 2입 등 (수량/용량 단위)
    r'\b\d+(?:\.\d+)?$' # 문장 끝에 혼자 남은 숫자 (마스크 3 등)
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
        (row["main_category"], row["sub_category"]): row["category_id"]
        for _, row in category_df.iterrows()
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
# 전처리 파이프라인
# ==========================================

def process_pipeline(
    df: pd.DataFrame,
    ac_automaton: ahocorasick.Automaton,
    synonym_dict: dict,
    category_df: pd.DataFrame = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Bronze raw DataFrame을 받아 silver / error DataFrame으로 전처리합니다.

    처리 흐름:
        [사전 준비]
        Step 1  - category_df로 lookup 딕셔너리 빌드

        [1루프: 행별 정제]
        Step 2  - product_id uuid 생성
        Step 3  - 제품명 기반 옵션 번들 필터링 → error
        Step 4  - 특수기호 제거 및 구분자 치환
        Step 5  - 유의어/오타 사전 치환
        Step 6  - 무효 문구 일괄 소거
        Step 7  - 이종 결합 번들 탐지 → error 또는 제거
        Step 8  - 성분명 농도/이명 괄호 제거
        Step 9  - 제품명 클리닝 + category_id 결정
        Step 10 - 중복 제거 (브랜드 + 정규화 이름 기준)

        [2루프: 성분 매칭]
        Step 11 - 성분명 내 쉼표 마스킹
        Step 12 - Aho-Corasick 탐색 + 히든 번들 탐지
        Step 13 - silver / error 라우팅

    Args:
        df:           Bronze raw DataFrame
        ac_automaton: 빌드된 Aho-Corasick 오토마타
        synonym_dict: 유의어/오타 매핑 딕셔너리
        category_df:  oliveyoung_category_master DataFrame (None이면 category_id=None)

    Returns:
        (silver_df, error_df)
    """
    from src.bronze_to_silver.ac_builder import search_with_ac

    # [Step 1] category lookup 딕셔너리 빌드
    category_lookup = build_category_lookup(category_df) if category_df is not None else {}

    interim_list  = []
    error_records = []

    # ── 1루프: 행별 정제 ────────────────────────────────────────
    for _, row in df.iterrows():
        raw_text         = str(row.get('ingredients', ''))
        product_name_raw = str(row.get('name', 'Unknown'))
        brand            = str(row.get('brand', 'Unknown'))
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

        if not raw_text or raw_text.strip() == 'nan':
            continue

        # [Step 2] product_id uuid 생성
        record_uuid = str(uuid.uuid4())

        # [Step 3] 제품명 기반 옵션 번들 필터링
        if REGEX_PRODUCT_OPTION_BUNDLE.search(product_name_raw):
            error_records.append(_make_error(
                record_uuid, category_id, brand, product_name_raw, product_name_raw, raw_text, url, crawled_at,
                'OPTION_BUNDLE_REJECTED', 'Multi-option product (n-종) detected in name'
            ))
            continue

        text = raw_text

        # [Step 4] 특수기호 제거 및 구분자 치환
        text = re.sub(r'[\r\n\t]', '', text)
        text = re.sub(r'[@|]', ',', text)

        # [Step 5] 유의어/오타 사전 치환
        for typo, correct in synonym_dict.items():
            if typo in text:
                text = text.replace(typo, correct)

        # [Step 6] 무효 데이터 및 안내 문구 일괄 소거
        text = REGEX_PREFIX_ALL.sub('', text)
        text = REGEX_LEGEND.sub('', text)
        text = REGEX_ILN.sub('', text)
        text = REGEX_LEGEND_WITH_BRACKET.sub('', text)
        text = REGEX_SYMBOLS.sub('', text)

        # [Step 7] 이종 결합 번들 탐지
        bundle_count = len(REGEX_BUNDLE.findall(text))
        if bundle_count >= 2:
            error_records.append(_make_error(
                record_uuid, category_id, brand, product_name_raw, product_name_raw, raw_text, url, crawled_at,
                'HETEROGENEOUS_BUNDLE_REJECTED', text
            ))
            continue
        elif bundle_count == 1:
            text = REGEX_BUNDLE.sub('', text)

        # [Step 8] 성분명 농도/이명 괄호 제거
        text = REGEX_BRACKET.sub('', text)

        # [Step 9] 제품명 클리닝
        product_name = REGEX_PRODUCT_BRACKET.sub(' ', product_name_raw)
        product_name = REGEX_PRODUCT_VOLUME_ANCHOR.sub('', product_name)
        product_name = REGEX_PRODUCT_MARKETING.sub('', product_name)
        product_name = REGEX_PRODUCT_TAIL_SYMBOLS.sub('', product_name)
        clean_product_name = REGEX_MULTI_SPACE.sub(' ', product_name).strip()

        interim_list.append({
            'product_id':              record_uuid,
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
            # 중복 제거 후 category_id 재조회 없이 쓰기 위해 보존
            '_main_category':          main_category,
            '_sub_category':           sub_category,
        })

    if not interim_list:
        return pd.DataFrame(), pd.DataFrame(error_records)

    # [Step 10] 중복 제거 (브랜드 + 정규화 이름 기준, 성분 문자열이 짧은 쪽 유지)
    interim_df = pd.DataFrame(interim_list)
    interim_df['text_len']  = interim_df['cleaned_text_str'].apply(len)
    interim_df['name_norm'] = interim_df['product_name'].str.replace(" ", "", regex=False)

    # 성분 문자열이 짧은 쪽을 우선순위로 두기 위해 정렬
    interim_df = interim_df.sort_values('text_len', ascending=True)

    # 1. 중복된 행 중 '첫 번째(살릴 것)'가 아닌 행들을 추출 (Keep='first')
    duplicate_mask = interim_df.duplicated(subset=['product_brand', 'name_norm'], keep='first')
    duplicates_to_error = interim_df[duplicate_mask]

    # 2. 중복 행들을 error_records에 추가
    for _, d_row in duplicates_to_error.iterrows():
        error_records.append({
            'product_id':              d_row['product_id'],
            'category_id':             d_row['category_id'],
            'product_brand':           d_row['product_brand'],
            'product_name_raw':        d_row['product_name_raw'],
            'product_name':            d_row['product_name'],
            'product_ingredients_raw': d_row['product_ingredients_raw'],
            'product_url':             d_row['product_url'],
            'crawled_at':              d_row['crawled_at'],
            'error_type':              'DUPLICATE_PRODUCT_REJECTED',
            'residual_text':           f"Duplicate of {d_row['product_brand']} | {d_row['product_name']}"
        })

    # 3. 중복 제거된 데이터프레임 생성 (Silver 진행용)
    deduped_df = interim_df[~duplicate_mask]

    # ── 2루프: 성분 매칭 ────────────────────────────────────────
    silver_records = []

    for _, row in deduped_df.iterrows():
        text        = row['cleaned_text_str']
        record_uuid = row['product_id']
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

        # [Step 12-2] 히든 번들 탐지 (핵심 성분 중복)
        if matches and (matches.count('정제수') >= 2 or matches.count('글리세린') >= 2):
            error_records.append({
                'product_id':              record_uuid,
                'category_id':             category_id,
                'product_brand':           row['product_brand'],
                'product_name_raw':        row['product_name_raw'],
                'product_name':            row['product_name'],
                'product_ingredients_raw': row['product_ingredients_raw'],
                'product_url':             url,
                'crawled_at':              crawled_at,
                'error_type':              'HIDDEN_BUNDLE_REJECTED',
                'residual_text':           'Duplicate Core Ingredients',
            })
            continue

        # [Step 12-3] 잔여 텍스트 마스킹 복원
        residual_text = residual.replace("_C_", ",").strip()

        # [Step 13] silver / error 라우팅
        if matches:
            silver_records.append({
                'product_id':              record_uuid,
                'category_id':             category_id,
                'product_brand':           row['product_brand'],
                'product_name':            row['product_name'],
                'product_ingredients':     matches,
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
            error_records.append({
                'product_id':              record_uuid,
                'category_id':             category_id,
                'product_brand':           row['product_brand'],
                'product_name_raw':        row['product_name_raw'],
                'product_name':            row['product_name'],
                'product_ingredients_raw': row['product_ingredients_raw'],
                'product_url':             url,
                'crawled_at':              crawled_at,
                'error_type':              'UNMAPPED_RESIDUAL',
                'residual_text':           residual_text,
            })

    return pd.DataFrame(silver_records), pd.DataFrame(error_records)


# ==========================================
# 내부 헬퍼
# ==========================================

def _make_error(
    product_id, category_id, brand, product_name_raw, product_name,
    raw_text, url, crawled_at,
    error_type, residual_text
) -> dict:
    """에러 레코드 딕셔너리를 생성합니다."""
    return {
        'product_id':              product_id,
        'category_id':             category_id,
        'product_brand':           brand,
        'product_name_raw':        product_name_raw,
        'product_name':            product_name,
        'product_ingredients_raw': raw_text,
        'product_url':             url,
        'crawled_at':              crawled_at,
        'error_type':              error_type,
        'residual_text':           residual_text,
    }