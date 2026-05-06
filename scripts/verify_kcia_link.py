"""
INCI → Iceberg KCIA 연결을 검증하는 드라이런 스크립트.

실행:
    cd Iceberg_pipeline
    python scripts/verify_kcia_link.py

S3 접근이 가능한 환경(EC2 또는 AWS 자격증명 설정)에서 실행해야 한다.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DuckDB, DataPath
from src.bronze_to_silver.ac_builder import load_kcia_mapping_dict


def main():
    print("=== KCIA S3 연결 검증 ===\n")

    print("1. DuckDB 커넥션 생성...")
    con = DuckDB.get_connection()

    print("2. 최신 KCIA S3 경로 탐색...")
    csv_path = DuckDB.get_latest_kcia_s3_path(con)
    print(f"   경로: {csv_path}\n")

    print("3. KCIA 매핑 딕셔너리 로드...")
    mapping = load_kcia_mapping_dict(csv_path, DataPath.KCIA_MAPPING_JSON)
    print(f"   로드된 키워드 수: {len(mapping)}\n")

    print("4. 샘플 5개:")
    for i, (key, std_name) in enumerate(list(mapping.items())[:5]):
        print(f"   [{i+1}] {key!r} → {std_name!r}")

    print("\n=== 검증 완료 ===")


if __name__ == "__main__":
    main()
