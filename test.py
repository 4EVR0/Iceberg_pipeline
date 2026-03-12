from pyiceberg.catalog import load_catalog

# 설정파일 기반 카탈로그 로드
catalog = load_catalog("default")

# 네임스페이스(DB) 출력
print(f"Connected to Glue Catalog. Databases: {catalog.list_namespaces()}")
