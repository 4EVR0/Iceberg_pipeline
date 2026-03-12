'''
전처리한 silver 테이블 체크하는 코드
'''

from pyiceberg.catalog.glue import GlueCatalog

# 1. 카탈로그 연결
catalog = GlueCatalog("oliveyoung_catalog", **{
    "s3.region": "ap-northeast-2",
    "uri": "https://glue.ap-northeast-2.amazonaws.com",
    "warehouse": "s3://oliveyoung-crawl-data/olive_young_silver/"
})

# 2. 테이블 로드 및 데이터 읽기
table = catalog.load_table("oliveyoung_db.oliveyoung_silver")
# Arrow Table로 읽은 후 Pandas DataFrame으로 변환
df = table.scan().to_arrow().to_pandas()

# 3. 결과 요약 출력
print(f"--- Silver Table Summary ---")
print(f"Total Count: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print("\n--- Sample Data (Top 5) ---")
print(df[['product_brand', 'product_name', 'product_ingredients']].head())

# 성분 데이터가 리스트 형태로 잘 들어갔는지 확인
print("\n--- Ingredient List Check ---")
print(f"First product ingredients type: {type(df['product_ingredients'].iloc[0])}")
print(f"First product ingredients: {df['product_ingredients'].iloc[0][:5]}...")

# 특정 브랜드 데이터만 확인
print()
print()
senka_silver = df[df['product_brand'] == '센카']
print(f"--- 센카 Silver 데이터 ({len(senka_silver)}건) ---")
print(senka_silver[['original_name', 'product_name']].head(10))