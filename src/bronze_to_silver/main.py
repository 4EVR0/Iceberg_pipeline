"""
Bronze → Silver 전처리 파이프라인 오케스트레이션

실행:
    cd Iceberg_pipeline
    python src/bronze_to_silver/main.py
"""

import sys
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pyiceberg.catalog import load_catalog

from config.settings import S3, Iceberg, DataPath, DuckDB
from src.bronze_to_silver.ac_builder  import load_kcia_mapping_dict, load_synonym_dict, build_ahocorasick
from src.bronze_to_silver.cleaner     import process_pipeline


def load_category_master():
    """
    Iceberg catalog에서 oliveyoung_category_master 테이블을 로드합니다.

    Returns:
        pd.DataFrame: category_id, main_category, sub_category 컬럼
    """
    catalog = load_catalog(
        Iceberg.CATALOG_NAME,
        **{
            "type":      "glue",
            "warehouse": S3.ICEBERG_METADATA_PATH,
            "s3.region": S3.REGION,
        }
    )
    table = catalog.load_table("oliveyoung_db.oliveyoung_category_master")
    return table.scan().to_arrow().to_pandas()


if __name__ == "__main__":
    print("=== Bronze → Silver 전처리 시작 ===\n")

    # Step 1. DuckDB 커넥션 (IAM Role → S3)
    print("1. DuckDB 커넥션 설정...")
    con = DuckDB.get_connection()

    # Step 2. sub_category별 최신 run_id 파일 목록 조회
    print("2. 최신 run_id bronze 파일 탐색...")
    try:
        latest_files = DuckDB.get_latest_bronze_files(con)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    # Step 3. Bronze 데이터 로드
    print(f"3. Bronze 데이터 로드 ({len(latest_files)}개 파일)...")
    try:
        file_list_sql = ", ".join(f"'{f}'" for f in latest_files)
        raw_df = con.execute(
            f"SELECT * FROM read_json_auto([{file_list_sql}], ignore_errors=true)"
        ).df()
    except Exception as e:
        print(f"[ERROR] JSON 로드 실패: {e}")
        sys.exit(1)
    print(f"   로드 완료: {len(raw_df)}건\n")

    # Step 4. category_master 로드
    print("4. category_master 로드...")
    try:
        category_df = load_category_master()
        print(f"   {len(category_df)}개 카테고리 로드됨\n")
    except Exception as e:
        # category_master 로드 실패 시 category_id=None으로 계속 진행
        print(f"[WARN] category_master 로드 실패 → category_id=None 으로 진행: {e}\n")
        category_df = None

    # Step 5. KCIA 성분 사전 로드 (캐시 없으면 CSV에서 생성)
    print("5. KCIA 성분 사전 준비...")
    kcia_dict = load_kcia_mapping_dict(
        csv_path        = DataPath.KCIA_CSV,
        json_cache_path = DataPath.KCIA_MAPPING_JSON,
        rebuild         = False,
    )
    print(f"   {len(kcia_dict)}개 키워드 로드됨\n")

    # Step 6. 유의어/오타 사전 로드
    print("6. 유의어 사전 로드...")
    synonym_dict = load_synonym_dict(DataPath.TYPO_MAP_JSON)

    # Step 7. Aho-Corasick 빌드
    print("\n7. Aho-Corasick 빌드...")
    ac_automaton = build_ahocorasick(kcia_dict)
    print("   빌드 완료\n")

    # Step 8. 전처리 파이프라인 실행
    print("8. 전처리 파이프라인 실행...")
    silver_df, error_df = process_pipeline(raw_df, ac_automaton, synonym_dict, category_df)
    print(f"   정상: {len(silver_df)}건 / 에러: {len(error_df)}건\n")

    # Step 9. Iceberg write → silver_pipeline/write_silver.py 에서 담당
    # TODO: silver_pipeline/write_silver.py 구현 후 연결
    print("9. Iceberg write → silver_pipeline/write_silver.py 에서 담당")

    print("\n=== 완료 ===")