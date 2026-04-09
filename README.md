# Iceberg Pipeline — Olive Young 성분 데이터 파이프라인

Olive Young 크롤링 데이터를 **Medallion Architecture (Bronze → Silver → Gold)** 로 처리하는 ETL 파이프라인입니다.
원시 제품 데이터에서 성분 정보를 정규화·집계하여 Apache Iceberg 테이블로 저장합니다.

---

## 아키텍처

```
Bronze (S3 JSON)
    ↓  DuckDB로 최신 run_id 파일 로드
Silver (Iceberg)
 ├── oliveyoung_silver_current    ← 최신 제품 데이터
 ├── oliveyoung_silver_history    ← 시계열 감사 로그
 └── oliveyoung_silver_error      ← 처리 실패 레코드 (DLQ)
    ↓  카테고리별 성분 집계
Gold (Iceberg)
 └── gold_ingredient_frequency    ← 카테고리별 Top 50 성분
    ↓
CSV 내보내기 (S3 data_csv/)
```

### 처리 흐름

1. **Load Bronze** — S3 glob으로 서브카테고리별 최신 `run_id` JSON 파일 탐색
2. **Load Metadata** — KCIA 성분 사전, 카테고리 마스터, 오타 사전, 불량 키워드 로드
3. **Clean** — 노이즈 제거, 성분 문자열 정규화, 번들 제품 감지, 오타 교정
4. **Match** — Aho-Corasick 자동화로 KCIA 표준명 매칭 (O(n+m))
5. **Split** — 정상 레코드 → Silver current/history, 오류 레코드 → Silver error
6. **Aggregate** — Gold 레이어에서 카테고리별 성분 빈도 집계
7. **Export** — Iceberg(Parquet) + S3 CSV 동시 저장

---

## 디렉토리 구조

```
Iceberg_pipeline/
├── config/
│   └── settings.py                    # AWS / S3 / Iceberg / DuckDB 설정
├── data/                              # 로컬 사전 파일
│   ├── kcia_ingredient_dict2.csv      # KCIA 표준 성분 사전 (6.5 MB)
│   ├── kcia_mapping_dict.json         # KCIA 매핑 캐시 (2.2 MB)
│   ├── typo_map.json                  # 정확 매칭 오타 사전
│   ├── typo_map_regex.json            # 정규식 기반 오타 패턴
│   └── garbage_keywords.json          # 불량 키워드 필터
├── models/
│   └── pipeline_models.py             # Dataclass: Dictionaries, ErrorRecord
├── src/bronze_to_silver/              # 핵심 ETL 파이프라인
│   ├── main.py                        # 실행 진입점
│   ├── pipeline.py                    # 오케스트레이션
│   ├── cleaner.py                     # 데이터 정제 로직
│   └── ac_builder.py                  # Aho-Corasick 자동화 빌더
├── silver_pipeline/                   # Iceberg 테이블 생성 & 쓰기
│   ├── schemas.py                     # 스키마 정의 (파티션·정렬 포함)
│   ├── create_silver.py               # Silver 테이블 초기화
│   ├── create_category_master.py      # 카테고리 마스터 테이블 생성
│   └── write_silver.py                # Pandas → Iceberg & S3 CSV 쓰기
├── gold_pipeline/                     # 집계 레이어
│   ├── create_gold_frequency_table.py # Gold 테이블 초기화
│   └── query_gold_frequency.py        # Gold 조회 (애드혹 분석)
├── src/gold_ingredient_frequency.py   # Gold 집계 실행 진입점
├── jupyter/                           # 탐색용 노트북
└── requirements.txt
```

---

## 주요 의존성

| 패키지 | 용도 |
|--------|------|
| `pandas` | 데이터 조작 |
| `pyahocorasick` | 고속 다중 문자열 매칭 |
| `pyiceberg` | Apache Iceberg 카탈로그 클라이언트 |
| `duckdb` | S3 데이터 SQL 쿼리 |
| `boto3` | AWS S3 작업 |
| `pyarrow` | Iceberg 쓰기용 Arrow 직렬화 |

---

## 환경 설정

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경 변수 설정

`.env` 파일 생성 (EC2 프로덕션 환경에서는 IAM Role로 대체):

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-northeast-2
S3_BUCKET=oliveyoung-crawl-data
```

---

## 실행 방법

### Iceberg 테이블 초기화 (최초 1회)

```bash
# Silver 테이블 생성
python silver_pipeline/create_silver.py

# 카테고리 마스터 테이블 생성
python silver_pipeline/create_category_master.py

# Gold 테이블 생성
python gold_pipeline/create_gold_frequency_table.py
```

### 파이프라인 실행

```bash
# Bronze → Silver 처리
python src/bronze_to_silver/main.py

# Silver → Gold 집계
python src/gold_ingredient_frequency.py
```

---

## Silver 테이블 스키마

### `oliveyoung_silver_current` / `oliveyoung_silver_history`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `category_id` | string | 카테고리 식별자 (파티션 키) |
| `product_id` | string | 제품 고유 ID |
| `product_brand` | string | 브랜드명 |
| `product_name` | string | 정제된 제품명 |
| `product_name_raw` | string | 원본 제품명 |
| `product_ingredients` | list\<string\> | 정규화된 성분 목록 |
| `product_ingredients_raw` | string | 원본 성분 문자열 |
| `rating` | float | 평점 |
| `review_count` | int | 리뷰 수 |
| `crawled_at` | timestamp | 크롤링 시각 (정렬 키) |
| `batch_job` | string | 배치 작업 ID |
| `batch_date` | date | 배치 처리 날짜 |

### `oliveyoung_silver_error` (DLQ)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `error_type` | string | 오류 분류 (파티션 키) |
| `residual_text` | string | 파싱 실패 잔여 텍스트 |
| (기타 공통 필드) | | |

**오류 타입 예시**: `INVALID_INGREDIENTS`, `INVALID_PRODUCT_NAME`, `DUPLICATE_PRODUCT_REJECTED`

---

## 인프라

- **스토리지**: AWS S3 (`oliveyoung-crawl-data`)
- **메타스토어**: AWS Glue Catalog (`oliveyoung_db`)
- **웨어하우스**: `s3://oliveyoung-crawl-data/olive_young_iceberg_metadata/`
- **리전**: `ap-northeast-2` (서울)
- **런타임**: Python 3.12, EC2 (IAM Role 기반 인증)

---

## 설계 결정 사항

### Current / History 이중 테이블
최신 데이터(`current`)와 시계열 감사 로그(`history`)를 분리하여 동시 쓰기.
재읽기 없이 시계열 추적 보장.

### Aho-Corasick 성분 매칭
6.5 MB KCIA 사전에 대해 O(n+m) 복잡도로 다중 성분명 동시 탐색.
쉼표 마스킹으로 성분명 내 쉼표 포함 케이스 처리.

### DLQ 패턴 (Dead Letter Queue)
처리 실패 레코드를 `silver_error`로 분리 저장.
데이터 손실 없이 사후 재처리·분석 가능.

### DuckDB + Glob으로 최신 데이터 탐색
`glob()` + 정규식 `run_id` 추출 + `GROUP BY max(run_id)` 로 서브카테고리별 최신 파일 자동 탐색.
경로 하드코딩 없이 멱등 실행 보장.
