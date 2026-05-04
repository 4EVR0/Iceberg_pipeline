# [Bronze → Silver] 올리브영 크롤링 데이터 전처리 파이프라인

태그: 아카이빙

> EC2 + Docker | Apache Iceberg + AWS Glue Catalog + DuckDB
> 

---

## 1. 개요

올리브영 크롤링 데이터를 메달리온 아키텍처(Bronze → Silver)로 처리하는 Iceberg 기반 데이터 파이프라인입니다.

| 구성 요소 | 기술 |
| --- | --- |
| 스토리지 | Amazon S3 (`oliveyoung-crawl-data`) |
| 테이블 포맷 | Apache Iceberg |
| 카탈로그 | AWS Glue Catalog |
| Bronze 읽기 | DuckDB |
| Silver 쓰기 | PyIceberg |
| 성분 매핑 | Aho-Corasick 오토마타 + KCIA 사전 |
| Reference 관리 | Iceberg 테이블 (typo_map, garbage_keywords, custom_ingredient_dict) |
| 실행 환경 | EC2 + Docker (Python 3.12) |

### S3 버킷 구조

```
s3://oliveyoung-crawl-data/
  ├── oliveyoung/                         ← Bronze (크롤링 raw JSON)
  │     └── {main_category}/
  │           └── {sub_category}/
  │                 └── run_id={YYYYMMDD_HHMMSS}/
  │                       └── part_000N.json
  ├── silver/
  │     ├── current/                      ← Silver current (최신 배치, overwrite)
  │     ├── history/                      ← Silver history (누적 이력, append)
  │     └── error/raw/                    ← Silver error (DLQ, overwrite)
  ├── reference/
  │     ├── typo_map/                     ← 오타/유의어 Iceberg 테이블
  │     ├── garbage_keywords/             ← 불량 키워드 Iceberg 테이블
  │     └── custom_ingredient_dict/       ← KCIA 미등재 성분 보정 Iceberg 테이블
  ├── olive_young_iceberg_metadata/       ← Iceberg 메타데이터
  ├── olive_young_gold/                   ← Gold (여기서는 자세히 표기 X)
  └── data_csv/                           ← 처리 결과 CSV 백업
```

---

## 2. 디렉토리 구조

```
Iceberg_pipeline/
  ├── Dockerfile                         ← 파이프라인 컨테이너 이미지 빌드
  ├── requirements.txt                   ← 파이프라인 의존성 (Docker 빌드용)
  ├── requirements-dev.txt               ← 분석·로컬 개발용 (jupyter 포함)
  ├── scripts/
  │     └── entrypoint.sh                ← 컨테이너 실행 모드 분기
  ├── dags/
  │     └── oliveyoung_pipeline.py       ← Airflow DAG (DockerOperator)
  ├── config/
  │     └── settings.py                  ← 전역 설정 (S3 경로, DuckDB 커넥션 등)
  ├── src/
  │     └── bronze_to_silver/
  │           ├── main.py                ← 실행 진입점
  │           ├── pipeline.py            ← 오케스트레이션 로직
  │           ├── cleaner.py             ← 13단계 전처리 파이프라인
  │           └── ac_builder.py          ← KCIA 사전 빌드 + Aho-Corasick 탐색
  ├── silver_pipeline/
  │     ├── create_silver.py             ← Iceberg 테이블 생성 (최초 1회)
  │     ├── create_category_master.py    ← 카테고리 마스터 테이블 생성
  │     └── write_silver.py              ← Iceberg write + CSV 백업
  ├── reference_pipeline/
  │     ├── create_reference_tables.py   ← Reference 테이블 생성 (최초 1회)
  │     ├── schemas.py                   ← Reference 스키마 정의
  │     └── sync_reference_data.py       ← JSON → Iceberg 동기화
  ├── gold_pipeline/
  └── data/
        ├── typo_map.json                ← 성분명 오타/유의어 사전 (Git 관리)
        ├── typo_map_regex.json          ← 정규식 기반 오타 패턴 (Git 관리)
        ├── garbage_keywords.json        ← 불량 키워드 필터 (Git 관리)
        ├── product_name_norm_map.json   ← 제품명 정규화 규칙 (Git 관리)
        └── custom_ingredient_dict.json  ← KCIA 미등재/오매핑 성분 보정 (Git 관리)
```

---

## 3. config/

### settings.py

EC2 전용 전역 설정 파일입니다. S3 경로, Glue 카탈로그, DuckDB 커넥션 헬퍼를 관리합니다.

| 클래스 / 함수 | 역할 |
| --- | --- |
| `S3` | 버킷명, 리전, bronze/silver/gold/data_csv/reference 경로 상수 |
| `OliveyoungIceberg` | Glue 카탈로그명, `oliveyoung_db` 테이블명 상수, `get_catalog()` |
| `INCIIceberg` | `inci_db` 테이블명 상수 (graphrag, gold ingredients 등), `get_catalog()` |
| `DataPath` | EC2 로컬의 JSON 사전 파일 경로 |
| `DuckDB.get_connection()` | httpfs/aws extension 설치 + IAM Role 인증 + S3 리전 설정이 완료된 커넥션 반환 |
| `DuckDB.get_latest_bronze_files(con)` | sub_category별 최신 run_id 파일 경로 목록 반환 |
| `OliveyoungIceberg.get_catalog()` | `oliveyoung_db` Glue 카탈로그 커넥션 반환 |
| `INCIIceberg.get_catalog()` | `inci_db` Glue 카탈로그 커넥션 반환 |

> 설정값(`REGION`, `BUCKET`, `DATABASE` 등)은 환경 변수 없이 코드에 직접 하드코딩합니다.

**`get_latest_bronze_files()` 동작 원리**

Bronze 경로는 `run_id=YYYYMMDD_HHMMSS` 형식이라 문자열 정렬이 날짜 정렬과 동일합니다. DuckDB glob으로 전체 파일 목록을 가져온 뒤 sub_category별로 `MAX(run_id)`를 뽑아 최신 파일만 필터링합니다.

---

## 4. src/

### bronze_to_silver/ac_builder.py

KCIA 원본 CSV를 읽어 Aho-Corasick 오토마타를 빌드하고, Iceberg Reference 테이블에서 오타/유의어 및 garbage 설정을 로드합니다.

| 함수 | 역할 |
| --- | --- |
| `generate_kcia_mapping_dict(inci_catalog)` | `inci_db.silver_kcia_cosing_graphrag_current` Iceberg 테이블 → `{검색키: 표준명칭}` 딕셔너리 생성. 표준명 + 구이명 모두 등록. 쉼표는 `_C_`로 마스킹 |
| `load_custom_ingredient_dict_from_iceberg(catalog)` | Iceberg `custom_ingredient_dict` 테이블에서 커스텀 성분 보정 목록 로드. `list[{"raw", "standard", "action"}]` 반환 |
| `apply_custom_ingredient_dict(mapping, entries)` | KCIA 딕셔너리에 커스텀 항목 적용. `add`는 미등록 키만 추가, `override`는 강제 덮어쓰기 |
| `load_typo_maps_from_iceberg(catalog)` | Iceberg `typo_map` 테이블에서 성분명 오타 사전 로드. `(typo_list, typo_regex_list)` 반환 |
| `load_product_name_norms_from_iceberg(catalog)` | Iceberg `typo_map` 테이블에서 `apply_to='product_name'` 행만 로드 |
| `load_garbage_config_from_iceberg(catalog)` | Iceberg `garbage_keywords` 테이블에서 불량 키워드 로드. `{"exact": [...], "contains": [...]}` 반환 |
| `build_ahocorasick(mapping)` | 매핑 딕셔너리로 Aho-Corasick 오토마타 빌드 |
| `search_with_ac(text, automaton)` | 텍스트에서 성분 탐색. 최장 일치 Greedy Filter 적용. `(매칭 리스트, 잔여 텍스트)` 반환 |

**KCIA 사전 로드**

```
INCIIceberg.get_catalog()로 inci_db 카탈로그 연결
  → silver_kcia_cosing_graphrag_current Iceberg 테이블 스캔
  → std_name_ko / old_name_ko 컬럼으로 매핑 딕셔너리 생성
  → custom_ingredient_dict 적용 (add / override)
  → Aho-Corasick 오토마타 빌드
```

> S3 CSV 직접 읽기 방식은 제거되었습니다. KCIA 데이터는 `inci_db.silver_kcia_cosing_graphrag_current` Iceberg 테이블에서 직접 로드합니다.
> 

**custom_ingredient_dict 적용 규칙**

KCIA에 없거나 잘못 매핑된 성분을 전처리 단계에서 직접 보정합니다. KCIA 사전 로드 직후, AC 빌드 직전에 적용됩니다.

| action | 동작 | 사용 케이스 |
| --- | --- | --- |
| `add` | KCIA에 해당 키가 없을 때만 추가 | CoSING 성분, 동의어(알코올→에탄올), KCIA 오타 보완 |
| `override` | KCIA에 있어도 강제 덮어쓰기 | 구명칭 충돌 수정 (레몬그라스추출물 오매핑 등) |

**Reference 테이블 로드 원칙**

오타 사전, 불량 키워드, 커스텀 성분 사전은 Git 관리 JSON → `sync_reference_data.py`로 Iceberg 동기화 → 파이프라인에서 Iceberg에서 읽습니다. 코드 재배포 없이 사전 업데이트가 가능합니다.

---

### bronze_to_silver/cleaner.py

Bronze raw DataFrame을 받아 13단계 전처리 후 silver / error DataFrame을 반환합니다.

### 처리 흐름

| 단계 | 설명 |
| --- | --- |
| Step 1 | 필수 필드 존재 여부 검증 (product_name, brand, url, ingredients, crawled_at, categories) → 없으면 `INCOMPLETE_DATA_REJECTED` |
| Step 2a | 제품명 기반 옵션 번들 필터링 (n종, n가지, 택n 등) → `OPTION_BUNDLE_REJECTED` |
| Step 2b | 불량 제품명 필터링 (garbage_keywords exact/contains 매칭) → `INVALID_METADATA_REJECTED` |
| Step 3 | 제품명 클리닝 (괄호·용량 앵커·마케팅 키워드 제거) + `product_id` UUID v5 생성 |
| Step 4 | 특수기호 정규화, `@ |
| Step 5 | 오타/유의어 사전 치환 (simple + regex_boundary) |
| Step 6 | 무효 문구 소거 (`전성분:`, `※표시`, `ILN` 코드 등) |
| Step 7 | 이종 결합 번들 탐지 (꺾쇠, 불릿, 넘버링 2개 이상) → `HETEROGENEOUS_BUNDLE_REJECTED` |
| Step 8 | 성분명 농도/이명 괄호 제거 (ppm, %, 유래 등) |
| Step 9 | 제품명 정규화 규칙 적용 + `category_id` 키워드 매칭으로 결정 |
| Step 10 | 중복 제거 (브랜드 + 정규화 이름 기준, 성분 문자열 짧은 쪽 유지) → 탈락 행은 `DUPLICATE_PRODUCT_REJECTED` |
| Step 11 | 성분명 내 쉼표 마스킹 (`_C_` 치환) |
| Step 12 | Aho-Corasick 탐색 + 히든 번들 탐지 (정제수 또는 글리세린 2회 이상 → `HIDDEN_BUNDLE_REJECTED`) |
| Step 13 | silver / error 라우팅. 잔여 텍스트에 한글/영문 포함 시 `UNMAPPED_RESIDUAL` |

### error_type 종류

| error_type | 발생 조건 |
| --- | --- |
| `INCOMPLETE_DATA_REJECTED` | 필수 필드 누락 (product_name, brand, url, ingredients, crawled_at, categories) |
| `OPTION_BUNDLE_REJECTED` | 제품명에 n종/n가지/택n 패턴 → 성분 데이터 신뢰 불가 |
| `INVALID_METADATA_REJECTED` | 제품명이 garbage_keywords 필터에 해당 |
| `HETEROGENEOUS_BUNDLE_REJECTED` | 성분 문자열에 번들 패턴 2개 이상 → 여러 제품 성분 혼재 |
| `HIDDEN_BUNDLE_REJECTED` | 정제수 또는 글리세린이 2회 이상 매칭 → 숨겨진 번들 |
| `UNMAPPED_RESIDUAL` | AC 탐색 후 잔여 텍스트에 한글/영문 포함 → 미매핑 성분 존재 |
| `DUPLICATE_PRODUCT_REJECTED` | 브랜드 + 정규화 제품명 기준 중복 제거 시 탈락한 행 |

---

### bronze_to_silver/pipeline.py + main.py

`main.py`는 진입점으로 `pipeline.py`의 `run_pipeline()`을 호출합니다. 비즈니스 로직은 모두 `pipeline.py`에 있습니다.

**실행 순서**

```
Step 1.  DuckDB 커넥션 설정 (IAM Role 인증 + S3 설정)
Step 2.  sub_category별 최신 run_id bronze 파일 탐색
Step 3.  Bronze JSON 데이터 로드
Step 4.  KCIA 성분 사전 로드 (inci_db.silver_kcia_cosing_graphrag_current Iceberg 테이블)
          → custom_ingredient_dict 적용 (add / override)
Step 5.  오타/유의어 사전 로드 (Iceberg typo_map)
Step 6.  제품명 정규화 규칙 로드 (Iceberg typo_map, apply_to='product_name')
Step 7.  garbage 키워드 설정 로드 (Iceberg garbage_keywords)
Step 8.  Aho-Corasick 오토마타 빌드
Step 9.  13단계 전처리 파이프라인 실행
Step 10. Iceberg 테이블에 write
          ├── silver_current: overwrite (최신 배치만 유지)
          ├── silver_history: append (누적 이력)
          └── silver_error: overwrite (최신 에러)
Step 11. S3 data_csv/ 에 CSV 백업 저장
```

---

## 5. reference_pipeline/

오타 사전, 불량 키워드를 Git 관리 JSON에서 Iceberg 테이블로 동기화합니다.

| 파일 | 역할 |
| --- | --- |
| `create_reference_tables.py` | `typo_map`, `garbage_keywords`, `custom_ingredient_dict` Iceberg 테이블 생성 (최초 1회) |
| `sync_reference_data.py` | `data/` 폴더의 JSON 파일 → Iceberg overwrite 동기화 |

**동기화 대상**

| JSON 파일 | 반영 대상 | 비고 |
| --- | --- | --- |
| `typo_map.json` | typo_map 테이블 | `apply_to='ingredient'` |
| `typo_map_regex.json` | typo_map 테이블 | `apply_to='ingredient'` |
| `product_name_norm_map.json` | typo_map 테이블 | `apply_to='product_name'` |
| `garbage_keywords.json` | garbage_keywords 테이블 | — |
| `custom_ingredient_dict.json` | custom_ingredient_dict 테이블 | `action='add'` / `'override'` |

**custom_ingredient_dict 스키마**

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| `raw` | string (**required**) | 사전에 등록할 검색 키 (제품 데이터 표기 기준) |
| `standard` | string | 매핑할 KCIA 표준명. `add` 시 자기 자신도 가능 |
| `action` | string (**required**) | `add` 또는 `override` |
| `reason` | string | 등록 사유 (CoSING 등재, 충돌 수정 등) |
| `synced_at` | timestamptz | 동기화 시각 |

**실행 방법**

```bash
# 최초 1회: 테이블 생성
python reference_pipeline/create_reference_tables.py

# 사전 업데이트 시: JSON 수정 후 동기화
python reference_pipeline/sync_reference_data.py
```

---

## 6. silver_pipeline/

### create_silver.py

`oliveyoung_silver_current`, `oliveyoung_silver_history`, `oliveyoung_silver_error` Iceberg 테이블을 Glue 카탈로그에 생성합니다. **최초 1회** 실행합니다.

### silver 테이블 스키마 (current / history 공통)

| 컬럼명 | 타입 | 설명 |
| --- | --- | --- |
| `category_id` | string (optional) | 카테고리 마스터 조인 결과 |
| `product_id` | string (**required**) | 브랜드명 + 정제된 제품명 기반 UUID v5, 
배치마다 재생성 |
| `product_brand` | string | 브랜드명 |
| `product_name` | string | 클리닝된 제품명 |
| `product_name_raw` | string | 원본 제품명 |
| `product_ingredients` | list<string> | 표준명칭으로 매핑된 성분 목록 |
| `product_ingredients_raw` | string | 원본 성분 문자열 |
| `rating` | float | 평점 |
| `review_count` | int | 리뷰 수 |
| `review_stats` | map<string, map<string, string>> | 리뷰 통계 (피부타입, 세정력 등) |
| `product_url` | string | 올리브영 상품 URL |
| `crawled_at` | timestamptz(UTC) | 크롤링 시각 |
| `batch_job` | string | 배치 작업 ID (schema evolution으로 자동 추가) |
| `batch_date` | timestamptz | 배치 처리 날짜 (schema evolution으로 자동 추가) |
- **파티션:** `category_id`
- **정렬:** `crawled_at ASC`
- **write 방식:** current → overwrite / history → append

### oliveyoung_silver_error 스키마 (DLQ)

| 컬럼명 | 타입 | 설명 |
| --- | --- | --- |
| `category_id` | string | 카테고리 ID |
| `product_id` | string (**required**) | UUID v5 |
| `product_brand` | string | 브랜드명 |
| `product_name_raw` | string | 원본 제품명 |
| `product_ingredients_raw` | string | 원본 성분 문자열 |
| `product_url` | string | 상품 URL |
| `crawled_at` | timestamptz(UTC) | 크롤링 시각 |
| `error_type` | string | 에러 유형 |
| `residual_text` | string | 에러 원인 텍스트 |
- **파티션:** `error_type`
- **write 방식:** overwrite

---

### write_silver.py

전처리 결과 DataFrame을 Iceberg 테이블에 저장하고 S3에 CSV 백업 파일을 저장합니다.

| 함수 | 역할 |
| --- | --- |
| `write_to_iceberg(silver_df, error_df)` | current overwrite + history append + error overwrite |
| `_load_and_evolve_table(catalog, identifier)` | 테이블 로드 → schema evolution(batch_job, batch_date 자동 추가) → reload 반환 |
| `_build_arrow_table_for_silver(df, table)` | Iceberg 테이블 스키마 기준으로 PyArrow Table 변환 |
| `_build_arrow_table_for_error(df, table)` | error DataFrame → PyArrow Table 변환 |
| `write_csv_to_s3(silver_df, error_df)` | S3 `data_csv/` 폴더에 타임스탬프 파일명으로 CSV 업로드 |

**CSV 저장 경로**

```
s3://oliveyoung-crawl-data/data_csv/oliveyoung_silver_current_{YYYYMMDD_HHMMSS}.csv
s3://oliveyoung-crawl-data/data_csv/oliveyoung_silver_error_{YYYYMMDD_HHMMSS}.csv
```

> `product_ingredients`는 list 타입이라 CSV에서는 `|`로 구분된 문자열로 직렬화됩니다.
> 

**PyArrow 변환 설계**

고정 스키마를 하드코딩하지 않고 `table.schema().as_arrow()`로 Iceberg 테이블 스키마를 기준으로 삼습니다. DataFrame에 없는 컬럼은 None으로 채우고, 컬럼 순서도 Iceberg 스키마 기준으로 맞춥니다.

| 컬럼 | 처리 |
| --- | --- |
| `product_ingredients` | list → `[str(v) for v in list]` 정규화 |
| `review_stats` | nested dict → `map<string, map<string,string>>` 변환 |
| `crawled_at`, `batch_date` | UTC timezone-aware datetime으로 변환 |

---

## 7. Docker

```bash
# 이미지 빌드
docker build -t oliveyoung-pipeline .

# Reference 테이블 동기화
docker run --network host oliveyoung-pipeline sync_reference

# Bronze → Silver 처리
docker run --network host oliveyoung-pipeline bronze_to_silver
```

- `-network host` 옵션으로 EC2 IAM Role 인증(IMDS `169.254.169.254`)을 컨테이너에서 그대로 사용합니다.

환경 변수 오버라이드가 필요할 경우:

```bash
docker run --network host \\
  -e S3_BUCKET=oliveyoung-crawl-data-staging \\
  -e ICEBERG_DATABASE=oliveyoung_db_dev \\
  oliveyoung-pipeline bronze_to_silver
```

---

## 8. Airflow 연동

`dags/oliveyoung_pipeline.py`에 DockerOperator 기반 DAG가 정의되어 있습니다.

```
sync_reference_data  →  bronze_to_silver
```

- `schedule=None` — 크롤링 DAG 완료 후 `TriggerDagRunOperator`로 트리거됩니다.
- 크롤링 DAG에 아래 task를 추가하면 자동 연결됩니다.

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

TriggerDagRunOperator(
    task_id="trigger_oliveyoung_pipeline",
    trigger_dag_id="oliveyoung_bronze_to_silver",
)
```

---

## 9. 데이터 조회 ⇒ jupyter/

PyIceberg로 직접 조회합니다.

### PyIceberg로 직접 조회

```python
import sys
sys.path.insert(0, "/shared/adv/iceberg-pipeline")

from config.settings import OliveyoungIceberg

catalog = OliveyoungIceberg.get_catalog()
table = catalog.load_table("oliveyoung_db.oliveyoung_silver_current")
df = table.scan().to_arrow().to_pandas()

df.head()
```

### 데이터 탐색 (예: 정규식 매핑되는 행 탐색)

```python
import re

regex_plus = r'\\d+\\s*\\+\\s*\\d+'

for idx, row in df[df["product_name_raw"].str.contains(regex_plus, regex=True, na=False)].iterrows():
    print(idx)
    print(row["product_name_raw"])
    print(row["product_name"])
    print()
```

---

## 10. 참고사항

### 최초 세팅 순서

```bash
# 1. Silver 테이블 생성
python silver_pipeline/create_silver.py
python silver_pipeline/create_category_master.py

# 2. Reference 테이블 생성 + 초기 데이터 동기화
python reference_pipeline/create_reference_tables.py
python reference_pipeline/sync_reference_data.py

# 3. Gold 테이블 생성
python gold_pipeline/create_gold_tables.py ingredient_frequency
python gold_pipeline/create_gold_tables.py product_change_log
```

### Reference 사전 업데이트

`data/` 폴더의 JSON 파일을 수정한 뒤 Iceberg에 동기화합니다.

```bash
# JSON 수정 후
python reference_pipeline/sync_reference_data.py
# 또는 Docker로
docker run --network host oliveyoung-pipeline sync_reference
```

### Schema Evolution

`batch_job`, `batch_date` 컬럼은 `write_silver.py`의 `_evolve_schema()`가 자동으로 추가합니다. 수동 재생성이 필요 없습니다.

### crawled_at timezone

크롤링 코드 수정 및 UTC로 수집·처리 완료. `timestamptz(UTC)` 타입으로 저장됩니다.