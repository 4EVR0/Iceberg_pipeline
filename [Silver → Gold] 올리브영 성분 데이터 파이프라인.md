# [Silver → Gold] 올리브영 성분 데이터 파이프라인

태그: 아카이빙

> EC2 + Docker | Apache Iceberg + AWS Glue Catalog + DuckDB
> 

---

## 1. 개요

올리브영 Silver 프로덕트 데이터와 INCI 성분 표준 데이터를 조인하여 Gold 레이어의 `gold_product_ingredients` 테이블을 생성하는 파이프라인입니다.

| 구성 요소 | 기술 |
| --- | --- |
| 스토리지 | Amazon S3 (`oliveyoung-crawl-data`) |
| 테이블 포맷 | Apache Iceberg |
| 카탈로그 | AWS Glue Catalog |
| Silver 읽기 | PyIceberg |
| INCI 데이터 읽기 | PyIceberg (`inci_db.gold_kcia_cosing_ingredients_current`) |
| Gold 쓰기 | PyIceberg |
| 조인 엔진 | DuckDB (in-process) |
| 실행 환경 | EC2 + Docker (Python 3.12) |

### 파이프라인 목적

`gold_product_ingredients`는 올리브영 프로덕트에 실제 사용된 **unique 성분 목록**에 INCI 표준 정보(표준명, 영문명, 기능, 규제)를 결합한 테이블입니다. **성분 효능 검색 및 논문 탐색**에 활용됩니다.

- **1행 = 1 unique 성분** (여러 프로덕트에 사용되어도 성분 기준 1행)
- Silver `product_ingredients`의 KCIA 한글 표준명 ↔ `inci_db.gold_kcia_cosing_ingredients_current` `kor_name` LEFT JOIN
- Write 방식: **overwrite** (Iceberg 스냅샷으로 타임트래블 지원)

### 데이터 소스 / 출력

| 구분 | 위치 |
| --- | --- |
| Silver 소스 | `oliveyoung_db.oliveyoung_silver_current` (Iceberg) |
| INCI 소스 | `inci_db.gold_kcia_cosing_ingredients_current` (Iceberg) |
| Gold 출력 | `oliveyoung_db.gold_product_ingredients` (Iceberg, S3: `olive_young_gold/gold_product_ingredients/`) |

---

## 2. 디렉토리 구조

이번 파이프라인에서 추가·변경된 파일만 표기합니다.

```
Iceberg_pipeline/
  ├── scripts/
  │     └── entrypoint.sh                ← silver_to_gold, create_gold_product_ingredients 커맨드 추가
  ├── dags/
  │     └── oliveyoung_pipeline.py       ← silver_to_gold 태스크 추가
  ├── config/
  │     └── settings.py                  ← INCI_GOLD_GLOB_PATTERN, GOLD_PRODUCT_INGREDIENTS_TABLE 추가
  ├── src/
  │     └── silver_to_gold/              ← (신규) Silver → Gold 오케스트레이션
  │           ├── __init__.py
  │           ├── main.py                ← 실행 진입점
  │           └── pipeline.py            ← 오케스트레이션 로직
  └── gold_pipeline/
        ├── schemas.py                   ← GOLD_PRODUCT_INGREDIENTS_SCHEMA 추가
        ├── create_gold_product_ingredients.py  ← (신규) 테이블 초기 생성
        ├── write_gold_product_ingredients.py   ← (신규) DuckDB 조인 + Iceberg write
        └── write_gold.py               ← _build_arrow from_pandas=True 수정
```

---

## 3. config/settings.py 변경 사항

### 추가된 설정

| 클래스 | 항목 | 값 |
| --- | --- | --- |
| `OliveyoungIceberg` | `GOLD_PRODUCT_INGREDIENTS_TABLE` | `"oliveyoung_db.gold_product_ingredients"` |
| `INCIIceberg` | `GOLD_INGREDIENTS_CURRENT_TABLE` | `"inci_db.gold_kcia_cosing_ingredients_current"` |

`INCIIceberg`는 `inci_db` 전체 테이블 상수와 `get_catalog()`를 담는 설정 클래스입니다. `OliveyoungIceberg`와 대칭 구조를 가집니다.

---

## 4. src/silver_to_gold/

### pipeline.py

Gold 레이어 쓰기 함수들을 호출하는 오케스트레이션 레이어입니다. `bronze_to_silver/pipeline.py`와 대칭 구조입니다.

**실행 순서**

```
Step 1.  Iceberg 카탈로그 연결
Step 2.  배치 ID / 배치 시각 생성 (UTC)
Step 3.  gold_product_ingredients 생성 (write_gold_product_ingredients 호출)
```

향후 Gold 테이블이 추가될 경우 이 파일에 write 함수 호출을 추가합니다.

### main.py

`pipeline.py`의 `run_pipeline()`을 호출하는 진입점입니다. `bronze_to_silver/main.py`와 동일한 패턴으로 `sys.path`를 프로젝트 루트로 설정합니다.

```bash
# 직접 실행
cd Iceberg_pipeline
python src/silver_to_gold/main.py
```

---

## 5. gold_pipeline/

### schemas.py — GOLD_PRODUCT_INGREDIENTS_SCHEMA

**테이블 설계 원칙:** `gold_product_ingredients`는 현재 시점의 성분 스냅샷 테이블입니다. 파티션 없이 overwrite 방식으로 운영하며, 과거 시점 데이터는 **Iceberg 자체 스냅샷**으로 타임트래블합니다.

| field_id | 컬럼명 | 타입 | 설명 |
| --- | --- | --- | --- |
| 1 | `ingredient_name` | string | KCIA 한글 표준명 (silver `product_ingredients` 값) |
| 2 | `inci_name` | string | INCI 표준명 (COSING uppercase) |
| 3 | `kor_name` | string | INCI CSV의 한글명 |
| 4 | `eng_name` | string | 영문명 |
| 5 | `cosing_functions` | string | 기능 목록 (`;` 구분자, 예: `SKIN CONDITIONING;HUMECTANT`) |
| 6 | `status` | string | Active / inactive |
| 7 | `cosmetic_restriction` | string | 화장품 규제 정보 (예: `V/1`) |
| 8 | `other_restrictions` | string | 기타 규제 정보 |
| 9 | `usage_count` | long | 이 성분을 사용하는 프로덕트 수 |
| 10 | `batch_job` | string | 배치 작업 ID |
| 11 | `batch_date` | timestamptz | 배치 처리 시각 |

- **파티션:** 없음
- **write 방식:** overwrite (`AlwaysTrue` 필터로 전체 교체)

> `batch_job`, `batch_date`는 현재 테이블 데이터가 어느 배치 결과인지 추적하는 용도입니다. `silver_current`와 동일한 설계 원칙입니다.

---

### create_gold_product_ingredients.py

Glue Catalog에 `oliveyoung_db.gold_product_ingredients` 테이블을 생성합니다. 기존 테이블이 있으면 drop 후 재생성합니다 (파티션 스펙 변경 시 활용).

```bash
python gold_pipeline/create_gold_product_ingredients.py
```

---

### write_gold_product_ingredients.py

Silver current 테이블에서 unique 성분을 추출하고 S3의 최신 INCI gold CSV와 LEFT JOIN하여 `gold_product_ingredients`에 overwrite합니다.

#### 핵심 함수

| 함수 | 역할 |
| --- | --- |
| `write_gold_product_ingredients(catalog, batch_job, batch_date)` | 메인 ETL 함수 |

#### DuckDB 쿼리 구조

```sql
WITH all_ingredients AS (
    -- silver_current의 product_ingredients 리스트 언네스트
    SELECT product_id, UNNEST(product_ingredients) AS ingredient_name
    FROM silver_arrow
    WHERE product_ingredients IS NOT NULL
),
unique_ingredients AS (
    -- 중복 제거 + 사용 프로덕트 수 집계
    SELECT ingredient_name, COUNT(DISTINCT product_id) AS usage_count
    FROM all_ingredients
    GROUP BY ingredient_name
)
-- INCI 데이터 LEFT JOIN (한글명 기준)
SELECT
    u.ingredient_name,
    i.inci_name, i.kor_name, i.eng_name,
    i.cosing_functions, i.status,
    i.cosmetic_restriction, i.other_restrictions,
    u.usage_count
FROM unique_ingredients u
LEFT JOIN inci_arrow i ON u.ingredient_name = i.kor_name
ORDER BY u.usage_count DESC, u.ingredient_name
```

#### 실행 흐름

```
Step 1.  silver_current Iceberg scan
          → product_id, product_ingredients 컬럼만 선택
Step 2.  INCIIceberg.get_catalog()로 inci_db 카탈로그 연결
Step 3.  gold_kcia_cosing_ingredients_current Iceberg scan
Step 4.  DuckDB: UNNEST → unique 성분 집계 → LEFT JOIN (silver_arrow ⋈ inci_arrow)
Step 5.  INCI 매핑 성공률 로그 출력
Step 6.  batch_job / batch_date 컬럼 추가
Step 7.  _build_arrow() → gold_table.overwrite(AlwaysTrue)
```

#### 매핑 성공률 로그

```
INFO: unique 성분: 2847건 | INCI 매핑 성공: 2301건 (80.8%)
```

> INCI 매핑 실패 행(inci_name IS NULL)은 DROP하지 않고 유지합니다. KCIA 성분이지만 INCI 미등재이거나, 커스텀 사전에서 추가된 성분이 이에 해당합니다.

---

### write_gold.py — `_build_arrow` 수정

`pa.array(values, type=field.type)` → `pa.array(values, type=field.type, from_pandas=True)`

LEFT JOIN 미매핑 행이나 UNNEST NULL 원소에서 pandas가 `float('nan')`을 삽입하는 경우, PyArrow가 기본적으로 이를 null로 처리하지 않아 `ArrowTypeError`가 발생합니다. `from_pandas=True`를 추가하면 `NaN` / `None` 모두 Arrow null로 정규화됩니다.

---

## 6. Docker

```bash
# Gold 테이블 최초 생성 (파티션 스펙 변경 시에도 재실행)
docker run --network host oliveyoung-pipeline create_gold_product_ingredients

# Silver → Gold 파이프라인 실행
docker run --network host oliveyoung-pipeline silver_to_gold
```

전체 파이프라인 순서:

```bash
docker run --network host oliveyoung-pipeline sync_reference
docker run --network host oliveyoung-pipeline bronze_to_silver
docker run --network host oliveyoung-pipeline silver_to_gold
```

---

## 7. Airflow 연동

`dags/oliveyoung_pipeline.py`에 `silver_to_gold` 태스크가 추가되었습니다.

```
sync_reference_data  →  bronze_to_silver  →  silver_to_gold
```

- `bronze_to_silver` 완료 후 자동 트리거됩니다.
- `schedule=None` — 크롤링 DAG의 `TriggerDagRunOperator`로 트리거됩니다.

---

## 8. 데이터 조회 ⇒ jupyter/

### PyIceberg로 직접 조회

```python
import sys
sys.path.insert(0, "/shared/adv/iceberg-pipeline")

from config.settings import OliveyoungIceberg

catalog = OliveyoungIceberg.get_catalog()
table = catalog.load_table("oliveyoung_db.gold_product_ingredients")
df = table.scan().to_arrow().to_pandas()

df.head()
```

### 활용 예시

```python
# INCI 매핑 성공률 확인
match_rate = df["inci_name"].notna().mean()
print(f"INCI 매핑 성공률: {match_rate:.1%}")

# 고빈도 성분 TOP 20 (논문 탐색 대상)
df.sort_values("usage_count", ascending=False).head(20)[
    ["ingredient_name", "inci_name", "eng_name", "cosing_functions", "usage_count"]
]

# 특정 기능 성분 필터링 (예: 피부 컨디셔닝)
df[df["cosing_functions"].str.contains("SKIN CONDITIONING", na=False)][
    ["ingredient_name", "inci_name", "eng_name", "usage_count"]
].sort_values("usage_count", ascending=False)

# Iceberg 타임트래블 (이전 배치 조회)
import pyiceberg.expressions as exp
snapshots = table.history()  # 스냅샷 목록 확인
df_prev = table.scan(snapshot_id=snapshots[-2].snapshot_id).to_arrow().to_pandas()
```

---

## 9. 참고사항

### 최초 세팅 순서

```bash
# Gold 테이블 생성
python gold_pipeline/create_gold_product_ingredients.py

# 파이프라인 실행 (silver_current가 이미 적재되어 있어야 함)
python src/silver_to_gold/main.py
```

### INCI 소스 전환 (CSV → Iceberg) — 완료

`inci_db.gold_kcia_cosing_ingredients_current` Iceberg 테이블이 생성됨에 따라 S3 CSV 기반 로딩에서 Iceberg 직접 로딩으로 전환되었습니다.

| 구분 | 변경 전 | 변경 후 |
| --- | --- | --- |
| INCI 소스 | `S3.INCI_GOLD_GLOB_PATTERN` (S3 CSV glob) | `INCIIceberg.GOLD_INGREDIENTS_CURRENT_TABLE` (Iceberg) |
| 로드 방식 | `DuckDB read_csv_auto` | `catalog.load_table().scan().to_arrow()` |
| 제거된 함수 | `_get_inci_s3_path(con)` | — |
| DuckDB JOIN 대상 | `inci_df` | `inci_arrow` |

### overwrite vs append 선택 근거

| | overwrite (채택) | append + batch_date 파티션 |
| --- | --- | --- |
| 현재 성분 목록 조회 | `SELECT *` | `WHERE batch_date = MAX(...)` 필터 필요 |
| 과거 시점 조회 | Iceberg 스냅샷 타임트래블 | SQL `WHERE batch_date = '...'` |
| 테이블 크기 | 항상 일정 (~3,000행) | 배치마다 누적 증가 |
| 적합한 테이블 성격 | 현재 상태 스냅샷 (dimension 성격) | 시계열 집계 (`gold_ingredient_frequency` 등) |

### INCI 소스 테이블 스키마 (`inci_db.gold_kcia_cosing_ingredients_current`)

| 컬럼명 | 설명 |
| --- | --- |
| `inci_name` | INCI 표준명 (uppercase, COSING 기준) |
| `kor_name` | 한글명 **(조인 키)** |
| `eng_name` | 영문명 |
| `cosing_functions` | 기능 목록 (`;` 구분자) |
| `status` | Active / inactive |
| `cosmetic_restriction` | 화장품 규제 정보 |
| `other_restrictions` | 기타 규제 정보 |
