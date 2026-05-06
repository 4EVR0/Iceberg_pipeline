'''
Gold 레이어 데이터 조회
oliveyoung_db.gold_ingredient_frequency: 카테고리별 성분 top 50
카테고리: TOTAL, 클렌징_클렌징폼_젤, 등 ...
'''

from pyiceberg.catalog.glue import GlueCatalog
import pandas as pd

from cosme_common import s3_paths


def query_all_categories_frequency(top_n=10):
    # 1. 카탈로그 연결
    catalog = GlueCatalog("oliveyoung_catalog", **{
        "s3.region": "ap-northeast-2",
        "uri": "https://glue.ap-northeast-2.amazonaws.com",
        "warehouse": s3_paths.GOLD_PATH,
    })

    # 2. 테이블 로드
    table = catalog.load_table("oliveyoung_db.gold_ingredient_frequency")

    # 3. 데이터 스캔 및 Pandas 변환
    df = table.scan().to_arrow().to_pandas()

    # 4. 모든 유니크한 카테고리 추출 (TOTAL 및 클렌징폼-젤 등)
    categories = df['category_id'].unique()

    # 5. 카테고리별 반복 조회 및 출력
    for cat in categories:
        # 해당 카테고리 필터링 및 정렬
        result = df[df['category_id'] == cat].sort_values('rank').head(top_n)
        
        print(f"==========================================")
        print(f"   [{cat}] 카테고리 성분 빈도 TOP {top_n}")
        print(f"==========================================")
        if result.empty:
            print("데이터가 없습니다.")
        else:
            print(result[['rank', 'ingredient_name', 'usage_count']].to_string(index=False))
        print("\n")

if __name__ == "__main__":
    query_all_categories_frequency(top_n=10)