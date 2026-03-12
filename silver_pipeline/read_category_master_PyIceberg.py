'''
pyiceberg로 category_master 테이블 조회하는 코드
'''

from pyiceberg.catalog.glue import GlueCatalog

# 1. 카탈로그 연결
catalog = GlueCatalog("oliveyoung_catalog", **{
    "s3.region": "ap-northeast-2",
    "uri": "https://glue.ap-northeast-2.amazonaws.com",
    "warehouse": "s3://oliveyoung-crawl-data/olive_young_silver/category_master/"
})

# 2. 테이블 로드
table = catalog.load_table("oliveyoung_db.oliveyoung_category_master")

# 3. 데이터를 Arrow Table로 읽기
df = table.scan().to_arrow()

# 4. 결과 출력
print(f"Total rows: {len(df)}")
print("-" * 30)
print(df.to_string()) # 전체 데이터 출력
