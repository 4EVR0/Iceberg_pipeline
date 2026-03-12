'''
silver 테이블에 있는 ingretients 만 가져오기
'''

import duckdb
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
import boto3
from io import StringIO
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_extract_unique_ingredients():
    # 1. 카탈로그 연결
    catalog = GlueCatalog("oliveyoung_catalog", **{
        "s3.region": "ap-northeast-2",
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "warehouse": "s3://oliveyoung-crawl-data/olive_young_silver/"
    })

    DATABASE_NAME = "oliveyoung_db"
    TABLE_NAME = "oliveyoung_silver"
    # 저장될 S3 경로 설정
    SAVE_PATH = "olive_young_gold/ingredient_master/" 

    try:
        # 2. Silver 테이블에서 유효한 데이터 스캔 (Orphan 파일 무시)
        logger.info(f"Silver 테이블({TABLE_NAME})에서 성분 데이터를 추출 중...")
        silver_table = catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")
        silver_arrow = silver_table.scan().to_arrow()

        # 3. DuckDB를 사용하여 중복 없는 성분 리스트 추출
        con = duckdb.connect()
        
        # unnest로 배열을 풀고, DISTINCT로 중복 제거 후 가나다순 정렬
        query = """
        SELECT DISTINCT 
            trim(unnested_table.ing) AS ingredient_name 
        FROM (
            SELECT unnest(product_ingredients) AS ing 
            FROM silver_arrow
        ) AS unnested_table
        WHERE unnested_table.ing IS NOT NULL AND unnested_table.ing <> ''
        ORDER BY ingredient_name ASC
        """
        
        ingredient_df = con.execute(query).df()
        logger.info(f"추출 완료: 총 {len(ingredient_df)}개의 고유 성분 발견.")

        # 4. S3에 CSV로 저장
        s3 = boto3.client('s3')
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"unique_ingredients_{now_str}.csv"
        
        csv_buffer = StringIO()
        ingredient_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        s3.put_object(
            Bucket='oliveyoung-crawl-data',
            Key=f"{SAVE_PATH}{file_name}",
            Body=csv_buffer.getvalue()
        )
        
        logger.info(f"성분 리스트 CSV 저장 완료: s3://oliveyoung-crawl-data/{SAVE_PATH}{file_name}")

    except Exception as e:
        logger.error(f"오류 발생: {e}")
        raise

if __name__ == "__main__":
    run_extract_unique_ingredients()