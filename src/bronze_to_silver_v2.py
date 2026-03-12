'''
올리브영 크롤링 데이터 전처리 로직
(로컬 테스트 목적... 개발 중...)

TODO
1. 전처리 로직 보완 마무리
    1) 제품명 클리닝 로직 보완
    2) 성분 오매핑 문제 보완
    3) 유의어 케이스는 저장된 json 파일 불러와서 처리하도록 분리
    4) 성분 사전 생성 로직 통합 (현재는 별개로 분리되어 있음 => generate_kcia_dict.py)
2. 새로운 크롤링 결과 기반으로 로직 수정 (리뷰 데이터 추가)
3. 리팩토링 (iceberg 상에서 실행될 수 있도록 수정, 카테고리 테이블 조인 로직 추가)
'''

import duckdb
import pandas as pd
import re
import uuid
import ahocorasick
import os
import json

# ==========================================
# 1. 정규표현식 상수 정의 (수정 및 확정된 패턴)
# ==========================================

# 2단계: 무효 데이터 및 안내 문구 제거
REGEX_PREFIX_ALL = re.compile(r'^전성분\s*:?\s*')
REGEX_LEGEND = re.compile(
    r'(?:(?:※|\*|\+)\s*(?:표시\s*:|자연\s*유래|식물\s*유래|유기농|ILN\d+)|'
    r'(?:※|\*|\+)?\s*(?:제공된\s*성분은|ILN\d+\s*성분\s*목록은))'
    r'.*?(?=\[|<|(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]|$)'
)
REGEX_ILN = re.compile(r'(?i)[<\[]?ILN\d+[>\]]?')
# REGEX_EXT_PPM = re.compile(r'\(?\s*(?:※|\*)?\s*\d+(?:,\d+)*\s*ppm\s*\)?')
REGEX_LEGEND_WITH_BRACKET = re.compile(r'\([^)]*[*※+][^)]*\)')
REGEX_SYMBOLS = re.compile(r'[*※+]')

# 3단계: 이종 결합 번들 탐지 (OR 결합)
REGEX_BUNDLE = re.compile(
    r'(?:<[^>]+>|'                         # 꺾쇠
    r'■\s*[^:■]+:\s*|'                     # 불릿과 콜론
    r'[가-힣a-zA-Z0-9]+\s*-\s*\[전성분|'   # 옵션명 - [전성분
    r'\[(?![Ii][Ll][Nn])[^\]]+\]|'         # 대괄호 (ILN 제외)
    r'(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]+)'          # 1) 제주녹차 등 넘버링 패턴
)

# 4단계: 성분명 농도/이명 괄호 제거
REGEX_BRACKET = re.compile(
    r'\([^)]*('
    r'ppm|ppb|%|mg|mG|G|g|ml|mL|'           # 단위
    r'NON\s*GMO|CI\s*\d+|BHA|AHA|'         # 특수 성분
    r'산\s*|성분|온천수|유래|추출물'         # 키워드
    r'|[\d.]+\s*'                          # 단순 숫자 농도 (예: 0.00004)
    r').*?\)'
)

# 5단계: 상품명 전처리
REGEX_PRODUCT_BRACKET = re.compile(r'\[.*?\]|\(.*?\)')
REGEX_PRODUCT_VOLUME = re.compile(
    r'\d+\s*(ml|g|매|호|ea)\s*([\+\*x]\s*\d+\s*(ml|g|매|호|ea)?)*',
    re.IGNORECASE
)
REGEX_PRODUCT_MARKETING = re.compile(
    r'기획|증정|단독|1\+1|본품|추가|2입|대용량|세트'
    r'|더블\s*(기획|세트|구성|\d+입|\d+\s*(ml|g))',
    re.IGNORECASE
)
REGEX_MULTI_SPACE = re.compile(r'\s+')

# 9단계: 쉼표 마스킹
REGEX_COMMA_MASK = re.compile(r'(?<=[A-Za-z0-9]),(?=[A-Za-z0-9])')

# ==========================================
# 2. Aho-Corasick 오토마타 빌더
# ==========================================
def build_ahocorasick(mapping_dict):
    A = ahocorasick.Automaton()
    for search_key, std_name in mapping_dict.items():
        A.add_word(search_key, (search_key, std_name))
    A.make_automaton()
    return A

def search_with_ac(processed_text, automaton):
    """
    공백이 제거되고 _C_ 마스킹된 텍스트에서 성분을 탐색하고 잔여물을 반환
    """
    if not processed_text:
        return [], ""

    matches = []
    # 1. 아호코라식 트리 순회하며 모든 매칭 결과 수집
    for end_idx, (matched_key, std_name) in automaton.iter(processed_text):
        start_idx = end_idx - len(matched_key) + 1
        matches.append((start_idx, end_idx, std_name))
    
    # 매칭 결과가 없으면 전체를 잔여물로 반환
    if not matches:
        return [], processed_text

    # 2. 최장 일치 기준 정렬 (시작 인덱스 순, 같으면 길이 내림차순)
    matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    
    final_ingredients = []
    matched_intervals = []
    last_end = -1
    
    # 3. 겹치는 구간 중 가장 적합한 성분만 선택 (Greedy Filter)
    for start, end, std_name in matches:
        if start > last_end:
            final_ingredients.append(std_name)
            matched_intervals.append((start, end))
            last_end = end
            
    # 4. 매칭된 구간을 제외한 나머지 글자(Residual) 추출
    residual = ""
    curr_pos = 0
    for start, end in matched_intervals:
        # 매칭 구간 이전의 텍스트를 잔여물에 추가
        residual += processed_text[curr_pos:start]
        curr_pos = end + 1
    # 마지막 매칭 이후의 남은 텍스트 추가
    residual += processed_text[curr_pos:]
    
    return final_ingredients, residual

# ==========================================
# 3. 전처리 핵심 파이프라인
# ==========================================
def process_pipeline(df, ac_automaton, category_df=None):
    silver_records = []
    error_records = []

    for _, row in df.iterrows():
        # 데이터 소스에 맞게 컬럼명 조정 필요 (예: ingredients, product_name, brand)
        raw_text= str(row.get('ingredients', ''))
        product_name_raw = str(row.get('name', 'Unknown'))
        brand = str(row.get('brand', 'Unknown'))
        url = str(row.get('url', ''))
        crawled_at = str(row.get('crawled_at', ''))
        main_category = str(row.get('main_category', ''))
        sub_category = str(row.get('sub_category', ''))
        
        if not raw_text or raw_text.strip() == 'nan':
            continue

        text = raw_text

        # [Step 1] 특수기호 제거 및 치환
        text = re.sub(r'[\r\n\t]', '', text)
        text = re.sub(r'[@|]', ',', text)

        # 알려진 오타 및 띄어쓰기 예외 케이스 사전 치환 (나중에 별도의 json 파일로 관리)
        typo_mapping = {
            "1,2헥산다이올": "1,2-헥산다이올",
            "1, 2-헥산다이올": "1,2-헥산다이올",
            "글루코놀락톤": "글루코노락톤",
            "글루코노락튼": "글루코노락톤",
            "글루코락톤": "글루코노락톤",
            "글루코노델타락톤": "글루코노락톤",
            "폴리글리세릴10다이스테아레이트": "폴리글리세릴-10다이스테아레이트",
            "폴리글리세릴10다이올리에이트": "폴리글리세릴-10다이올리에이트",
            "폴리글리세릴 6다이카프레이트": "폴리글리세릴-6다이카프레이트",
            "폴리글리세릴3다이스테아레이트": "폴리글리세릴-3다이스테아레이트"
        }
        for typo, correct in typo_mapping.items():
            if typo in text:
                text = text.replace(typo, correct)

        # [Step 2] 무효 데이터 및 안내 문구 일괄 소거
        text = REGEX_PREFIX_ALL.sub('', text)
        text = REGEX_LEGEND.sub('', text)
        text = REGEX_ILN.sub('', text)
        # text = REGEX_EXT_PPM.sub('', text)
        text = REGEX_LEGEND_WITH_BRACKET.sub('', text)
        text = REGEX_SYMBOLS.sub('', text)

        # [Step 3] 이종 결합 번들 에러 처리 (Count 분기)
        bundle_matches = REGEX_BUNDLE.findall(text)
        bundle_count = len(bundle_matches)

        if bundle_count >= 2:
            error_records.append({
                'product_brand': brand,
                'product_name_raw': product_name_raw,
                'product_ingredients_raw': raw_text,
                'product_URL': url,
                'crawled_at': crawled_at,
                'error_type': 'HETEROGENEOUS_BUNDLE_REJECTED',
                'residual_text': text
            })
            continue 
        elif bundle_count == 1:
            text = REGEX_BUNDLE.sub('', text)

        # [Step 4] 성분명 농도/이명 표기 제거
        text = REGEX_BRACKET.sub('', text)

        # [Step 5] 제품명 클리닝
        product_name = REGEX_PRODUCT_BRACKET.sub('', product_name_raw)
        product_name = REGEX_PRODUCT_VOLUME.sub('', product_name)
        product_name = REGEX_PRODUCT_MARKETING.sub('', product_name)
        clean_product_name = REGEX_MULTI_SPACE.sub(' ', product_name).strip()
        
        # product_id uuid 생성
        record_uuid = str(uuid.uuid4())

        # [Step 9] 성분명 내 쉼표 마스킹
        text = REGEX_COMMA_MASK.sub('_C_', text)

        # [Step 10] 성분 데이터 Split 및 매칭
        extracted_ingredients = []
        residual_text_list = []

        # 1. 텍스트 분할 및 공백 제거 (사용자 제안 로직)
        if ',' in text:
            # 실제 구분자(쉼표)로 분할 후 각 성분명에서 공백 제거
            parts = [p.replace(" ", "") for p in text.split(',')]
            # 다시 쉼표로 합쳐서 아호코라식 탐색용 텍스트 생성
            processed_text = ",".join(parts)
        else:
            # 쉼표가 없는 경우 공백만 제거
            processed_text = text.replace(" ", "")

        # 2. 아호코라식 탐색 (사전의 Key는 공백 제거 및 _C_ 마스킹된 상태여야 함)
        # search_with_ac 함수는 매칭된 리스트와 남은 텍스트를 반환
        matches, residual = search_with_ac(processed_text, ac_automaton)
        
        if matches:
            extracted_ingredients.extend(matches)
        
        if residual and len(residual.strip()) > 1:
            residual_text_list.append(residual)

        # 추출된 성분 중복 검사를 통한 히든 번들 탐지
        if extracted_ingredients:
            # 베이스 성분 중복 시 이종 결합으로 간주하고 즉시 에러로 라우팅
            if extracted_ingredients.count('정제수') >= 2 or extracted_ingredients.count('글리세린') >= 2:
                error_records.append({
                    'product_id': record_uuid,
                    'product_brand': brand,
                    'product_name_raw': product_name_raw,
                    'product_ingredients_raw': raw_text,
                    'product_URL': url,
                    'crawled_at': crawled_at,
                    'error_type': 'HIDDEN_BUNDLE_REJECTED',
                    'residual_text': 'Duplicate Core Ingredients Detected'
                })
                continue # 정상 silver_records 적재를 건너뜀

        # 3. 최종 잔여 텍스트 병합 및 마스킹 복원
        residual_text = ", ".join(residual_text_list).replace("_C_", ",")

        # [Step 11] 부분 성공 데이터 분할 라우팅
        if extracted_ingredients:
            silver_records.append({
                'product_id': record_uuid,
                'product_brand': brand,
                'product_name': clean_product_name,
                'product_name_raw': product_name_raw,
                'product_ingredients': extracted_ingredients,
                'product_ingredients_raw': raw_text,
                'product_URL': url,
                'crawled_at': crawled_at
            })
        
        if residual_text and len(residual_text.strip()) > 2 and re.search(r'[가-힣a-zA-Z0-9]', residual_text):
            error_records.append({
                'product_id': record_uuid,
                'product_brand': brand,
                'product_name_raw': product_name_raw,
                'product_ingredients_raw': raw_text,
                'product_URL': url,
                'crawled_at': crawled_at,
                'error_type': 'UNMAPPED_RESIDUAL',
                'residual_text': residual_text
            })

    silver_df = pd.DataFrame(silver_records)
    error_df = pd.DataFrame(error_records)

    # [Step 6] 중복 제품 제거
    if not silver_df.empty:
        silver_df['ing_len'] = silver_df['product_ingredients'].apply(len)
        silver_df = silver_df.sort_values('ing_len').drop_duplicates(subset=['product_brand', 'product_name'], keep='first')
        silver_df = silver_df.drop(columns=['ing_len'])

    # # [Step 7] 카테고리 테이블 조인 (category_df가 제공된 경우)
    # if category_df is not None and not silver_df.empty:
    #     silver_df = pd.merge(silver_df, category_df, on='clean_product_name', how='left')

    return silver_df, error_df

# ==========================================
# 4. 로컬 실행 및 파일 I/O
# ==========================================
if __name__ == "__main__":
    # 로컬 입력/출력 경로 설정 (S3 구조 모방)
    # 실제 로컬에 구축해둔 JSON 파일 경로 패턴으로 수정해야 합니다.
    LOCAL_BRONZE_PATTERN = "./oliveyoung_서연/클렌징/*/*/*.json" 
    OUTPUT_SILVER_CSV = "./output/silver_data.csv"
    OUTPUT_ERROR_CSV = "./output/silver_error.csv"

    print("1. 로컬 Bronze 데이터 로드 시작...")
    con = duckdb.connect()
    
    try:
        # JSON 파일을 읽어 데이터프레임으로 변환
        raw_df = con.execute(f"SELECT * FROM read_json_auto('{LOCAL_BRONZE_PATTERN}', ignore_errors=true)").df()
    except Exception as e:
        print(f"JSON 로드 실패. 에러: {e}")
        # 테스트용 임시 대체: raw_ingredients_20260304_194928.csv 사용
        # raw_df = pd.read_csv('raw_ingredients_20260304_194928.csv')
    
    print(f"데이터 로드 완료: 총 {len(raw_df)}건")

    # KCIA 성분 데이터
    with open('kcia_mapping_dict.json', 'r', encoding='utf-8') as f:
        kcia_mapping_dict = json.load(f)

    print("2. Aho-Corasick 사전 빌드...")
    ac_automaton = build_ahocorasick(kcia_mapping_dict)

    print("3. 전처리 파이프라인 실행...")
    silver_table, error_table = process_pipeline(raw_df, ac_automaton)

    print("4. 결과 CSV 저장...")
    silver_table.to_csv(OUTPUT_SILVER_CSV, index=False, encoding='utf-8-sig')
    error_table.to_csv(OUTPUT_ERROR_CSV, index=False, encoding='utf-8-sig')

    print(f"완료. 정상 데이터 {len(silver_table)}건, 에러 데이터 {len(error_table)}건 저장됨.")
    print(f"저장 경로:\n - {os.path.abspath(OUTPUT_SILVER_CSV)}\n - {os.path.abspath(OUTPUT_ERROR_CSV)}")