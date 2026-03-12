'''
duckDB로 category_master 테이블 조회하는 코드
'''

import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL aws;")
con.execute("SET s3_region='ap-northeast-2';")

# 시스템(EC2 IAM Role)의 자격 증명을 사용하도록 로드
con.execute("SET s3_region='ap-northeast-2';")
con.execute("SET s3_use_ssl=true;")
# 아래 설정이 중요합니다. EC2 인스턴스 프로필을 사용하게 합니다.
con.execute("SET s3_url_style='path';") 

# 혹은 환경 변수로 이미 로드되어 있다면 자동으로 읽어오지만, 
# 명시적으로 아래 커맨드를 통해 로드할 수도 있습니다.
con.execute("LOAD aws;") # 'aws' 확장이 설치되어 있어야 함
con.execute("CALL load_aws_credentials();")

# S3에 적재된 Parquet 파일을 직접 조회
query = """
SELECT * FROM read_parquet('s3://oliveyoung-crawl-data/olive_young_silver/category_master/data/*.parquet')
"""

result = con.execute(query).df()
print(result)