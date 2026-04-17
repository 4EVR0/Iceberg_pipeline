"""
Iceberg Pipeline 전역 설정 파일 (EC2 전용)

사용법:
    from config.settings import S3, Iceberg, DataPath, DuckDB
"""

import os
import duckdb
from pyiceberg.catalog import load_catalog

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Iceberg_pipeline/


# ==========================================
# AWS / S3 경로 설정
# ==========================================
class S3:
    REGION = "ap-northeast-2"
    BUCKET = "oliveyoung-crawl-data"

    # Bronze: s3://.../oliveyoung/main_category/sub_category/run_id=YYYYMMDD_HHMMSS/part_*.json
    BRONZE_PREFIX       = "oliveyoung"
    BRONZE_GLOB_PATTERN = f"s3://{BUCKET}/{BRONZE_PREFIX}/*/*/run_id=*/*.json"  # glob 탐색용

    # KCIA: INCI_data_silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv
    KCIA_PREFIX       = "INCI_data_silver/kcia_cosing"
    KCIA_GLOB_PATTERN = f"s3://{BUCKET}/{KCIA_PREFIX}/batch=*/kcia_cosing_graphrag_map.csv"

    # Silver
    SILVER_CURRENT_PATH  = f"s3://{BUCKET}/silver/current/"
    SILVER_HISTORY_PATH  = f"s3://{BUCKET}/silver/history/"
    SILVER_ERROR_PATH    = f"s3://{BUCKET}/silver/error/raw/"
    CATEGORY_MASTER_PATH = f"s3://{BUCKET}/olive_young_category_master/"

    # Gold
    GOLD_PATH = f"s3://{BUCKET}/olive_young_gold/"

    # Iceberg 메타데이터
    ICEBERG_METADATA_PATH = f"s3://{BUCKET}/olive_young_iceberg_metadata/"

    # 처리 결과 CSV 저장 (조회용)
    DATA_CSV_PATH = f"s3://{BUCKET}/data_csv/"

    # Reference Data (typo_map, garbage_keywords)
    REFERENCE_TYPO_MAP_PATH         = f"s3://{BUCKET}/reference/typo_map/"
    REFERENCE_GARBAGE_KEYWORDS_PATH = f"s3://{BUCKET}/reference/garbage_keywords/"


# ==========================================
# Glue Catalog / Iceberg 설정
# ==========================================
class Iceberg:
    CATALOG_NAME          = "glue"
    DATABASE              = "oliveyoung_db"
    SILVER_CURRENT_TABLE  = f"{DATABASE}.oliveyoung_silver_current"
    SILVER_HISTORY_TABLE  = f"{DATABASE}.oliveyoung_silver_history"
    SILVER_ERROR_TABLE    = f"{DATABASE}.oliveyoung_silver_error"
    CATEGORY_MASTER_TABLE              = f"{DATABASE}.oliveyoung_category_master"
    GOLD_INGREDIENT_FREQUENCY_TABLE    = f"{DATABASE}.gold_ingredient_frequency"
    GOLD_PRODUCT_CHANGE_LOG_TABLE      = f"{DATABASE}.gold_product_change_log"
    TYPO_MAP_TABLE                     = f"{DATABASE}.typo_map"
    GARBAGE_KEYWORDS_TABLE             = f"{DATABASE}.garbage_keywords"

    @staticmethod
    def get_catalog():
        return load_catalog(
            Iceberg.CATALOG_NAME,
            **{
                "type":      "glue",
                "warehouse": S3.ICEBERG_METADATA_PATH,
                "s3.region": S3.REGION,
            }
        )


# ==========================================
# 데이터 파일 경로 (EC2 로컬 디스크)
# ==========================================
class DataPath:
    DATA_DIR              = os.path.join(_BASE_DIR, "data")
    KCIA_CSV              = os.path.join(DATA_DIR, "kcia_ingredient_dict2.csv")
    TYPO_MAP_JSON              = os.path.join(DATA_DIR, "typo_map.json")
    TYPO_MAP_REGEX_JSON        = os.path.join(DATA_DIR, "typo_map_regex.json")
    GARBAGE_KEYWORDS_JSON      = os.path.join(DATA_DIR, "garbage_keywords.json")
    PRODUCT_NAME_NORM_MAP_JSON = os.path.join(DATA_DIR, "product_name_norm_map.json")

    

# ==========================================
# DuckDB 설정
# ==========================================
class DuckDB:
    @staticmethod
    def get_connection() -> duckdb.DuckDBPyConnection:
        """
        S3 읽기용 DuckDB 커넥션을 반환합니다.
        IAM Role이 EC2 인스턴스에 연결되어 있어야 합니다.
        """
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL aws;   LOAD aws;")
        con.execute("CALL load_aws_credentials();")
        con.execute(f"SET s3_region='{S3.REGION}';")
        return con

    @staticmethod
    def get_latest_bronze_files(con: duckdb.DuckDBPyConnection) -> list[str]:
        """
        sub_category별로 가장 최신 run_id에 해당하는 JSON 파일 경로 목록을 반환합니다.

        S3 구조:
            oliveyoung/{main_category}/{sub_category}/run_id={YYYYMMDD_HHMMSS}/{part_*.json}

        동작:
            1. BRONZE_GLOB_PATTERN으로 전체 파일 목록 조회
            2. sub_category별 max(run_id) 선택  ← 문자열 정렬로 최신값 결정
            3. 최신 run_id에 해당하는 파일 경로만 반환

        Returns:
            list[str]: 최신 run_id 파일 경로 목록
                       예) ['s3://.../클렌징/오일-밤/run_id=20260312_140917/part_0001.json', ...]

        Raises:
            RuntimeError: S3에서 파일을 찾지 못한 경우
        """
        df = con.execute(f"""
            WITH all_files AS (
                SELECT
                    file,
                    regexp_extract(file, '/([^/]+)/([^/]+)/run_id=([^/]+)/', 1) AS main_category,
                    regexp_extract(file, '/([^/]+)/([^/]+)/run_id=([^/]+)/', 2) AS sub_category,
                    regexp_extract(file, 'run_id=([^/]+)/',                  1) AS run_id
                FROM glob('{S3.BRONZE_GLOB_PATTERN}')
            ),
            latest_runs AS (
                SELECT sub_category, max(run_id) AS latest_run_id
                FROM all_files
                GROUP BY sub_category
            )
            SELECT f.file
            FROM all_files    f
            JOIN latest_runs  l
              ON f.sub_category = l.sub_category
             AND f.run_id       = l.latest_run_id
            ORDER BY f.main_category, f.sub_category, f.file
        """).df()

        if df.empty:
            raise RuntimeError(
                f"S3에서 bronze 파일을 찾지 못했습니다.\n"
                f"패턴: {S3.BRONZE_GLOB_PATTERN}"
            )

        files = df['file'].tolist()
        print(f"   최신 run_id 파일 {len(files)}개 선택됨")
        return files


    @staticmethod
    def get_latest_kcia_s3_path(con: duckdb.DuckDBPyConnection) -> str:
        """
        S3에서 batch=YYYY-MM 파티션 중 가장 최신 batch의 KCIA CSV 경로를 반환합니다.

        S3 구조:
            INCI_data_silver/kcia_cosing/batch=YYYY-MM/kcia_cosing_matched_final.csv

        Returns:
            str: 최신 batch CSV의 S3 경로

        Raises:
            RuntimeError: S3에서 파일을 찾지 못한 경우
        """
        df = con.execute(f"""
            SELECT
                file,
                regexp_extract(file, 'batch=([^/]+)/', 1) AS batch_id
            FROM glob('{S3.KCIA_GLOB_PATTERN}')
            ORDER BY batch_id DESC
            LIMIT 1
        """).df()

        if df.empty:
            raise RuntimeError(
                f"S3에서 KCIA CSV 파일을 찾지 못했습니다.\n"
                f"패턴: {S3.KCIA_GLOB_PATTERN}"
            )

        path = df['file'].iloc[0]
        batch_id = df['batch_id'].iloc[0]
        print(f"   KCIA 최신 batch: {batch_id} ({path})")
        return path