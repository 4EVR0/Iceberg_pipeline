import duckdb
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
import boto3
from io import BytesIO, StringIO
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_bronze_to_silver():
    # DuckDB 설정 및 AWS 인증 로드
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")
    con.execute("CALL load_aws_credentials();")
    con.execute("SET s3_region='ap-northeast-2';")

    # 설정 값 (테스트 목적으로 일단 클렌징폼-젤 카테고리만 진행)
    BRONZE_PATH = "s3://oliveyoung-crawl-data/oliveyoung/클렌징/*/*/*.json"
    MASTER_PATH = "s3://oliveyoung-crawl-data/olive_young_silver/category_master/data/*.parquet"
    SILVER_PATH = "s3://oliveyoung-crawl-data/olive_young_silver/"
    SILVER_CSV_PATH = "olive_young_silver/data_csv/"
    ERROR_S3_PATH = "olive_young_silver_error/"
    DATABASE_NAME = "oliveyoung_db"
    TABLE_NAME = "oliveyoung_silver"

    logger.info("정제 프로세스를 시작합니다...")

    # 통합 전처리 SQL
    # 순서: 영어 통일 -> 번들 판별/격리 -> 순수 상품명 추출 -> 중복 제거 -> ID 생성 및 Split
    transform_query = f"""
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
    ),
    category_joined AS (
        SELECT 
            c.category_id AS category_id,
            d.*
        FROM deduplicated d
        JOIN read_parquet('{MASTER_PATH}') c
          ON d.main_category = c.main_category 
         AND d.sub_category = c.sub_category
    )
    SELECT 
        category_id,
        CAST(gen_random_uuid() AS VARCHAR) AS product_id,
        product_brand,
        product_name,
        original_name,
        list_filter(
            list_transform(
                -- 숫자 뒤에 오지 않는 쉼표(구분자)를 '@@@' 같은 특수 구분자로 치환 후 split
                -- 규칙: 숫자와 숫자 사이의 쉼표는 그대로 두고, 그 외의 쉼표는 구분자로 인식
                string_to_array(
                    regexp_replace(ingredients, ',([^0-9])', '@@@\1', 'g'), 
                    '@@@'
                ), 
                x -> trim(x)
            ), 
            x -> length(x) > 1 AND x <> '' -- '1' 같은 노이즈 제거
        ) AS product_ingredients,
        product_url,
        CAST(crawled_at AS TIMESTAMP) AS crawled_at,
        data_status
    FROM category_joined
    """

    try:
        # 가공 데이터 실행
        full_df = con.execute(transform_query).df()

        # 데이터 분리
        silver_df = full_df[full_df['data_status'] == 'NORMAL'].drop(columns=['data_status'])
        error_df = full_df[full_df['data_status'] != 'NORMAL']

        
        s3 = boto3.client('s3')

        # Silver 적재 (Iceberg & CSV)
        if not silver_df.empty:
            # Iceberg 적재
            catalog = GlueCatalog("oliveyoung_catalog", **{
                "s3.region": "ap-northeast-2",
                "uri": "https://glue.ap-northeast-2.amazonaws.com",
                "warehouse": SILVER_PATH
            })
            table = catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")

            arrow_table = pa.Table.from_pandas(silver_df, preserve_index=False)
            table.overwrite(arrow_table)
            logger.info(f"Silver 적재 완료: {len(silver_df)}건 (Deduplicated)")

            # CSV 저장
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_file_name = f"{TABLE_NAME}_{now_str}.csv"
            
            # CSV 데이터를 메모리 버퍼에 작성
            csv_buffer = StringIO()
            silver_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            
            s3.put_object(
                Bucket='oliveyoung-crawl-data',
                Key=f"{SILVER_CSV_PATH}{csv_file_name}",
                Body=csv_buffer.getvalue()
            )
            logger.info(f"Silver CSV 백업 완료: s3://oliveyoung-crawl-data/{SILVER_CSV_PATH}{csv_file_name}")

        # Error 적재 (S3 Parquet & CSV)
        if not error_df.empty:
            # Parquet 저장
            pq_buffer = BytesIO()
            error_df.to_parquet(pq_buffer, index=False)
            error_pq_key = f"{ERROR_S3_PATH}{TABLE_NAME}/error_{now_str}.parquet"
            s3.put_object(
                Bucket='oliveyoung-crawl-data',
                Key=error_pq_key,
                Body=pq_buffer.getvalue()
            )

            # CSV 저장
            error_csv_buffer = StringIO()
            error_df.to_csv(error_csv_buffer, index=False, encoding='utf-8-sig')
            error_csv_key = f"{ERROR_S3_PATH}{TABLE_NAME}/error_{now_str}.csv"
            
            s3.put_object(
                Bucket='oliveyoung-crawl-data',
                Key=error_csv_key,
                Body=error_csv_buffer.getvalue()
            )
            logger.info(f"Error 데이터 격리 및 CSV 저장 완료: {len(error_df)}건")
            logger.info(f"Error CSV 경로: s3://{ERROR_S3_PATH}{error_csv_key}")

    except Exception as e:
        logger.error(f"오류 발생: {e}")
        raise

if __name__ == "__main__":
    run_bronze_to_silver()