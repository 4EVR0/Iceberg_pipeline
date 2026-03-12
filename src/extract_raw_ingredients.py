'''
rawdata(bronze)에 있는 ingredients 만 추출
'''

import duckdb
import boto3
from io import StringIO
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_extract_raw_ingredients_asis():
    # 1. DuckDB 및 AWS 설정
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    con.execute("SET s3_region='ap-northeast-2';")

    # 2. 경로 설정
    BRONZE_PATH = 's3://oliveyoung-crawl-data/oliveyoung/클렌징/*/*/*.json'
    SAVE_PATH_PREFIX = "olive_young_gold/raw_ingredients_asis/"
    BUCKET_NAME = 'oliveyoung-crawl-data'

    logger.info("Raw JSON에서 성분 데이터를 가공 없이 추출합니다.")

    # 3. 데이터 추출 SQL (전처리 없음)
    # 성분 문자열 전체를 하나의 단위로 보고 중복만 제거합니다.
    extract_query = f"""
    WITH raw_data AS (
        SELECT 
            main_category,
            sub_category,
            brand AS product_brand,
            name AS original_name,
            ingredients,
            url AS product_url,
            strptime(crawled_at, '%Y-%m-%dT%H:%M:%S.%f') AS crawled_at
        FROM read_json_auto('{BRONZE_PATH}')
    ),
    classified_data AS (
        SELECT 
            *,
            CASE 
                -- [격리] 성분 데이터에 [, -, 1) 등 구분자가 있는 경우 (이종 결합 확률 높음)
                WHEN ingredients LIKE '[%' OR ingredients LIKE '-%' OR ingredients LIKE '1)%' 
                THEN 'MIXED_BUNDLE_ERROR'

                -- [격리] 이름에 사은품 결합 기호 '(+' 가 있는 경우
                WHEN original_name LIKE '%(+%' 
                THEN 'GIFT_BUNDLE_ERROR'

                -- [조건부 허용] '기획' 문구가 있어도 1+1, 더블, 2입 등 동일 제품 번들은 NORMAL로 취급
                WHEN original_name LIKE '%기획%' THEN
                    CASE 
                        WHEN regexp_matches(original_name, '1\+1|더블|2입|2개입|대용량') THEN 'NORMAL'
                        ELSE 'PROMOTION_CHECK_REQUIRED'
                    END
                
                ELSE 'NORMAL'
            END AS data_status,
            -- 순수 상품명 추출 (마케팅 용어 및 용량 제거)
            -- 1. 대괄호/소괄호 내용 제거
            -- 2. 복합 용량(숫자+단위+기호+숫자) 패턴 제거: \d+\s*(ml|g|매|호|ea)\s*([\+\*x]\s*\d+\s*(ml|g|매|호|ea)?)*
            -- 3. 마케팅 단어(기획, 세트 등) 제거
            trim(
                regexp_replace(
                    regexp_replace(original_name, 
                        '\[.*?\]|\(.*?\)|\d+\s*(ml|g|매|호|ea)\s*([\+\*x]\s*\d+\s*(ml|g|매|호|ea)?)*|기획|증정|단독|1\+1|본품|추가|더블|2입|대용량|세트', 
                        '', 
                        'gi'
                    ),
                    '\s+', ' ', 'g' -- 다중 공백을 단일 공백으로 치환
                )
            ) AS product_name
        FROM raw_data
    ),
    deduplicated AS (
        SELECT *
        FROM classified_data
        -- 중복 제거: 브랜드 + 정제 상품명 + 성분 기준
        -- 가장 처음 크롤링된 레코드 선정
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY product_brand, product_name, hash(ingredients) 
            ORDER BY crawled_at ASC
        ) = 1
    )
    SELECT DISTINCT 
        ingredients 
    FROM deduplicated
    WHERE ingredients IS NOT NULL
      AND data_status = 'NORMAL'
    ORDER BY ingredients ASC
    """

    try:
        # 데이터프레임 변환
        df = con.execute(extract_query).df()
        logger.info(f"추출 완료: 총 {len(df)}건의 고유 성분 문자열 식별.")

        # 4. S3 저장 (CSV & JSON)
        s3 = boto3.client('s3')
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # A. CSV 저장 (utf-8-sig 적용)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{SAVE_PATH_PREFIX}raw_ingredients_{now_str}.csv",
            Body=csv_buffer.getvalue()
        )

        # B. JSON 저장
        json_data = df['ingredients'].to_json(orient='records', force_ascii=False)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{SAVE_PATH_PREFIX}raw_ingredients_{now_str}.json",
            Body=json_data
        )

        logger.info(f"저장 완료: s3://{BUCKET_NAME}/{SAVE_PATH_PREFIX}")

    except Exception as e:
        logger.error(f"오류 발생: {e}")
        raise

if __name__ == "__main__":
    run_extract_raw_ingredients_asis()