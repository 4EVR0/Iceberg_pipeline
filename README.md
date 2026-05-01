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
├── Dockerfile                         # 파이프라인 컨테이너 이미지 빌드
├── requirements.txt                   # 파이프라인 의존성 (Docker 빌드용)
├── requirements-dev.txt               # 분석·로컬 개발용 의존성 (jupyter 포함)
├── scripts/
│   └── entrypoint.sh                  # 컨테이너 실행 모드 분기
├── dags/
│   └── oliveyoung_pipeline.py         # Airflow DAG (DockerOperator)
├── config/
│   └── settings.py                    # AWS / S3 / Iceberg / DuckDB 설정
├── data/                              # 로컬 Reference JSON 파일
│   ├── typo_map.json                  # 정확 매칭 오타 사전
│   ├── typo_map_regex.json            # 정규식 기반 오타 패턴
│   ├── product_name_norm_map.json     # 제품명 정규화 규칙
│   ├── garbage_keywords.json          # 불량 키워드 필터
│   └── custom_ingredient_dict.json    # KCIA 미등재 성분 보정
├── models/
│   └── pipeline_models.py             # Dataclass: Dictionaries, ErrorRecord
├── src/bronze_to_silver/              # 핵심 ETL 파이프라인
│   ├── main.py                        # 실행 진입점
│   ├── pipeline.py                    # 오케스트레이션
│   ├── cleaner.py                     # 데이터 정제 로직
│   └── ac_builder.py                  # Aho-Corasick 자동화 빌더
├── reference_pipeline/                # Reference 데이터 관리
│   ├── schemas.py
│   ├── create_reference_tables.py
│   └── sync_reference_data.py         # JSON → Iceberg 동기화
├── silver_pipeline/                   # Iceberg 테이블 생성 & 쓰기
│   ├── schemas.py                     # 스키마 정의 (파티션·정렬 포함)
│   ├── create_silver.py               # Silver 테이블 초기화
│   ├── create_category_master.py      # 카테고리 마스터 테이블 생성
│   └── write_silver.py                # Pandas → Iceberg & S3 CSV 쓰기
├── gold_pipeline/                     # 집계 레이어
│   ├── create_gold_frequency_table.py # Gold 테이블 초기화
│   └── query_gold_frequency.py        # Gold 조회 (애드혹 분석)
├── src/gold_ingredient_frequency.py   # Gold 집계 실행 진입점
└── jupyter/                           # 탐색용 노트북
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
| `s3fs` | pandas S3 직접 읽기 |

분석·로컬 개발 시에는 `requirements-dev.txt`를 사용합니다 (`ipykernel` 포함).

---

## 환경 변수

`config/settings.py`는 아래 환경 변수를 읽으며, 미설정 시 괄호 안의 기본값을 사용합니다.

| 환경 변수 | 기본값 | 용도 |
|-----------|--------|------|
| `AWS_DEFAULT_REGION` | `ap-northeast-2` | AWS SDK 리전 |
| `S3_BUCKET` | `oliveyoung-crawl-data` | S3 버킷명 |
| `ICEBERG_DATABASE` | `oliveyoung_db` | Glue Catalog 데이터베이스명 |

EC2 프로덕션 환경에서는 IAM Role이 자동으로 AWS 인증을 처리하므로 별도 키 설정이 필요 없습니다.

---

## 실행 방법

### Iceberg 테이블 초기화 (최초 1회)

```bash
python silver_pipeline/create_silver.py
python silver_pipeline/create_category_master.py
python reference_pipeline/create_reference_tables.py
python gold_pipeline/create_gold_frequency_table.py
python reference_pipeline/sync_reference_data.py
```

### EC2에서 직접 실행

```bash
# Reference 테이블 동기화
python reference_pipeline/sync_reference_data.py

# Bronze → Silver 처리
python src/bronze_to_silver/main.py

# Silver → Gold 집계
python src/gold_ingredient_frequency.py
```

---

## Docker

### 이미지 빌드

```bash
docker build -t oliveyoung-pipeline .
```

### 컨테이너 실행

EC2 IAM Role 인증을 그대로 사용하기 위해 `--network host`로 실행합니다.

```bash
# Reference 테이블 초기 생성 (최초 1회 또는 신규 테이블 추가 시)
docker run --network host oliveyoung-pipeline create_reference_tables

# Reference 테이블 동기화
docker run --network host oliveyoung-pipeline sync_reference

# Bronze → Silver 처리
docker run --network host oliveyoung-pipeline bronze_to_silver
```

환경 변수를 오버라이드하려면 `-e` 플래그를 사용합니다.

```bash
docker run --network host \
  -e S3_BUCKET=oliveyoung-crawl-data-staging \
  -e ICEBERG_DATABASE=oliveyoung_db_dev \
  oliveyoung-pipeline bronze_to_silver
```

---

## Airflow 연동

`dags/oliveyoung_pipeline.py`에 DockerOperator 기반 DAG가 정의되어 있습니다.

```
sync_reference_data  →  bronze_to_silver
```

- `schedule=None` — 크롤링 DAG 완료 후 `TriggerDagRunOperator`로 트리거됩니다.
- 크롤링 DAG에 아래 task를 추가하면 자동 연결됩니다.

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger = TriggerDagRunOperator(
    task_id="trigger_oliveyoung_pipeline",
    trigger_dag_id="oliveyoung_bronze_to_silver",
)
```

DAG 파일은 Airflow의 `dags/` 경로에 배포하면 UI에서 바로 확인할 수 있습니다.

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

**오류 타입**: `INCOMPLETE_DATA_REJECTED` · `OPTION_BUNDLE_REJECTED` · `INVALID_METADATA_REJECTED` · `HETEROGENEOUS_BUNDLE_REJECTED` · `DUPLICATE_PRODUCT_REJECTED` · `UNMAPPED_RESIDUAL` · `HIDDEN_BUNDLE_REJECTED`

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

### Reference 테이블 분리 관리
오타 사전·불량 키워드·커스텀 성분 보정을 Git 관리 JSON으로 편집하고, `sync_reference_data.py`로 Iceberg에 동기화.
코드 재배포 없이 사전 업데이트 가능.

`custom_ingredient_dict`는 KCIA 미등재 성분 추가(`add`)와 오매핑 수정(`override`) 두 가지 action을 지원한다.
