import os
from pyiceberg.catalog import load_catalog

os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-2"

# 파일 대신 딕셔너리로 설정을 직접 전달
catalog = load_catalog(
    "default",
    **{
        "type": "glue",
        "s3.region": "ap-northeast-2",
        "warehouse": "s3://oliveyoung-crawl-data/olive_young_silver/",
    }
)

# 이제 정상적으로 출력되어야 합니다.
print(f"Connected! Databases: {catalog.list_namespaces()}")
