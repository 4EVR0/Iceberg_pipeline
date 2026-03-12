'''
카테고리 테이블 생성 스크립트 (최초 1회, 혹은 이후 수정 시 실행)
- 카테고리 데이터를 DataFrame 형태로 생성 및 S3에 저장
- iceberg 메타데이터(스냅샷) 생성 (oliveyoung_category_master)
'''

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog

# 1. 데이터 정의
CATEGORIES = {
    "스킨케어": ["스킨/토너", "에센스/세럼/앰플", "크림", "로션","미스트/오일"],
    "마스크팩": ["시트팩", "패드", "페이셜팩", "코팩", "패치"],
    "클렌징": ["클렌징폼/젤", "오일/밤", "워터/밀크", "필링&스크럽"],
    "더모 코스메틱": ["스킨케어", "바디케어", "클렌징", "선케어", "마스크팩"],
    "맨즈케어": ["스킨케어"]
}

# 2. 데이터 평면화 및 PyArrow 변환
flat_data = []
for main, subs in CATEGORIES.items():
    for sub in subs:
        cat_id = f"{main}_{sub}".replace(" ", "").replace("/", "-")
        flat_data.append({"category_id": cat_id, "main_category": main, "sub_category": sub})

df = pd.DataFrame(flat_data)
arrow_table = pa.Table.from_pandas(df) # PyIceberg는 Arrow 형식을 받습니다.

# 3. Iceberg Catalog 연결
catalog = GlueCatalog("oliveyoung_catalog", **{
    "s3.region": "ap-northeast-2",
    "uri": "https://glue.ap-northeast-2.amazonaws.com",
    "warehouse": "s3://oliveyoung-crawl-data/olive_young_silver/category_master/"
})

# 4. Iceberg 테이블 로드 및 데이터 추가 (메타데이터 자동 관리)
table = catalog.load_table("oliveyoung_db.oliveyoung_category_master")
table.append(arrow_table)

print("Category master data successfully appended to Iceberg table.")
