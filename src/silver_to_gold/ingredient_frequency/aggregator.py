"""
Silver → Gold 성분 빈도 집계 모듈

담당:
    - Silver Iceberg 테이블에서 유효 스냅샷 데이터 로드
    - category_id별 + 전체(TOTAL) 성분 사용 빈도 집계
    - rank 계산 (TOP 50)
    - gold DataFrame 반환
"""

import duckdb
import pandas as pd

from config.settings import Iceberg


# ==========================================
# Silver 데이터 로드
# ==========================================

def load_silver_arrow():
    """
    Iceberg 메타데이터가 보증하는 silver 테이블의 현재 유효한 스냅샷을 Arrow로 로드합니다.
    S3 parquet을 직접 읽으면 삭제된 파일까지 중복 계산되므로 반드시 scan()을 사용합니다.

    Returns:
        pyarrow.Table
    """
    catalog = Iceberg.get_catalog()
    silver_table = catalog.load_table(Iceberg.SILVER_TABLE)
    return silver_table.scan().to_arrow()


# ==========================================
# 집계
# ==========================================

def aggregate_ingredient_frequency(silver_arrow) -> pd.DataFrame:
    """
    Silver Arrow Table로부터 카테고리별 + 전체 성분 사용 빈도를 집계합니다.

    집계 규칙:
        - product_ingredients(list) unnest 후 카테고리별 COUNT
        - 'TOTAL' 카테고리로 전체 합산도 함께 생성
        - 1글자 이하 또는 숫자만으로 구성된 성분명 필터링
        - category_id별 usage_count 내림차순, 동점이면 ingredient_name 오름차순으로 rank 부여
        - rank <= 50 (Iceberg.GOLD_INGREDIENT_FREQUENCY_TOP_N) 만 유지

    Args:
        silver_arrow: load_silver_arrow()가 반환한 pyarrow.Table

    Returns:
        pd.DataFrame: columns = [category_id, ingredient_name, usage_count, rank]
    """
    con = duckdb.connect()

    gold_df = con.execute(f"""
        WITH unnested_data AS (
            SELECT
                category_id,
                unnest(product_ingredients) AS ingredient_name
            FROM silver_arrow
        ),
        filtered_data AS (
            -- 1글자 이하 또는 숫자만으로 구성된 노이즈 제거
            SELECT * FROM unnested_data
            WHERE length(ingredient_name) > 1
              AND ingredient_name NOT SIMILAR TO '[0-9]+'
        ),
        category_counts AS (
            SELECT
                category_id,
                ingredient_name,
                COUNT(*) AS usage_count
            FROM filtered_data
            GROUP BY category_id, ingredient_name
        ),
        total_counts AS (
            SELECT
                'TOTAL'          AS category_id,
                ingredient_name,
                COUNT(*)         AS usage_count
            FROM filtered_data
            GROUP BY ingredient_name
        ),
        combined_counts AS (
            SELECT * FROM category_counts
            UNION ALL
            SELECT * FROM total_counts
        ),
        ranked_ingredients AS (
            SELECT
                category_id,
                ingredient_name,
                usage_count,
                -- Iceberg 스키마(IntegerType)와 일치시키기 위해 CAST
                CAST(ROW_NUMBER() OVER (
                    PARTITION BY category_id
                    ORDER BY usage_count DESC, ingredient_name ASC
                ) AS INTEGER) AS rank
            FROM combined_counts
        )
        SELECT
            category_id,
            ingredient_name,
            usage_count,
            rank
        FROM ranked_ingredients
        WHERE rank <= {Iceberg.GOLD_INGREDIENT_FREQUENCY_TOP_N}
        ORDER BY category_id, rank
    """).df()

    print(f"   집계 완료: {len(gold_df)}개 레코드 (카테고리별 TOP {Iceberg.GOLD_INGREDIENT_FREQUENCY_TOP_N} + TOTAL)")
    return gold_df