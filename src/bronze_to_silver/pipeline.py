"""
Bronze → Silver 전처리 파이프라인 오케스트레이션 로직
"""

import sys
from dataclasses import dataclass

import ahocorasick

from config.settings import Iceberg, DataPath, DuckDB
from src.bronze_to_silver.ac_builder import (
    load_kcia_mapping_dict,
    load_typo_maps,
    load_garbage_config,
    build_ahocorasick,
)
from src.bronze_to_silver.cleaner import process_pipeline
from silver_pipeline.write_silver import write_to_iceberg, write_csv_to_s3


def load_category_master():
    """
    Iceberg catalog에서 oliveyoung_category_master 테이블을 로드합니다.

    Returns:
        pd.DataFrame: category_id, main_category, sub_category 컬럼
    """
    catalog = Iceberg.get_catalog()
    table = catalog.load_table(Iceberg.CATEGORY_MASTER_TABLE)
    return table.scan().to_arrow().to_pandas()


def load_bronze_data(con):
    """
    DuckDB 커넥션으로 최신 run_id bronze 파일을 로드합니다.

    Returns:
        pd.DataFrame: bronze raw 데이터
    """
    print("2. 최신 run_id bronze 파일 탐색...")
    try:
        latest_files = DuckDB.get_latest_bronze_files(con)
    except RuntimeError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

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

    return raw_df


@dataclass
class Dictionaries:
    ac_automaton:    ahocorasick.Automaton
    typo_list:       list[dict]
    typo_regex_list: list[dict]
    garbage_config:  dict


def load_dictionaries() -> Dictionaries:
    """
    KCIA 사전, 유의어/오타 사전, garbage 설정, Aho-Corasick 오토마타를 준비합니다.
    """
    print("5. KCIA 성분 사전 준비...")
    kcia_dict = load_kcia_mapping_dict(
        csv_path        = DataPath.KCIA_CSV,
        json_cache_path = DataPath.KCIA_MAPPING_JSON,
    )
    print(f"   {len(kcia_dict)}개 키워드 로드됨\n")

    print("6. 유의어/오타 사전 로드...")
    typo_list, typo_regex_list = load_typo_maps(
        typo_map_path       = DataPath.TYPO_MAP_JSON,
        typo_map_regex_path = DataPath.TYPO_MAP_REGEX_JSON,
    )

    print("\n7. garbage 키워드 설정 로드...")
    garbage_config = load_garbage_config(DataPath.GARBAGE_KEYWORDS_JSON)

    print("\n8. Aho-Corasick 빌드...")
    ac_automaton = build_ahocorasick(kcia_dict)
    print("   빌드 완료\n")

    return Dictionaries(
        ac_automaton    = ac_automaton,
        typo_list       = typo_list,
        typo_regex_list = typo_regex_list,
        garbage_config  = garbage_config,
    )


def run_pipeline():
    """Bronze → Silver 전처리 파이프라인 전체를 실행합니다."""
    print("=== Bronze → Silver 전처리 시작 ===\n")

    print("1. DuckDB 커넥션 설정...")
    con = DuckDB.get_connection()

    raw_df = load_bronze_data(con)

    print("4. category_master 로드...")
    try:
        category_df = load_category_master()
        print(f"   {len(category_df)}개 카테고리 로드됨\n")
    except Exception as e:
        print(f"[WARN] category_master 로드 실패 → category_id=None 으로 진행: {e}\n")
        category_df = None

    dicts = load_dictionaries()

    print("9. 전처리 파이프라인 실행...")
    silver_df, error_df = process_pipeline(
        df              = raw_df,
        ac_automaton    = dicts.ac_automaton,
        typo_list       = dicts.typo_list,
        typo_regex_list = dicts.typo_regex_list,
        category_df     = category_df,
        garbage_config  = dicts.garbage_config,
    )
    print(f"   정상: {len(silver_df)}건 / 에러: {len(error_df)}건\n")

    print("10. Iceberg write...")
    write_to_iceberg(silver_df, error_df)

    print("\n11. CSV 저장 (s3 data_csv/)...")
    write_csv_to_s3(silver_df, error_df)

    print("\n=== 완료 ===")
