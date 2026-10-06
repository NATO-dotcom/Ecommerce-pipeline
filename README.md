# 🛒 Olist E-Commerce Data Pipeline

A batch ETL pipeline built with **Apache Spark (PySpark)** that processes the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) through a **Medallion Architecture** (Bronze → Silver → Gold). The pipeline ingests raw CSV files, enforces explicit schemas, applies data-quality cleaning rules, and produces analytics-ready Gold tables.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Dataset Description](#dataset-description)
- [Project Structure](#project-structure)
- [Pipeline Stages — Step by Step](#pipeline-stages--step-by-step)
  - [Stage 0: Exploration (explore.ipynb)](#stage-0-exploration-exploreipynb)
  - [Stage 1: Schema Definition (schemas.py)](#stage-1-schema-definition-schemaspy)
  - [Stage 2: Bronze Ingestion (ingest.py)](#stage-2-bronze-ingestion-ingestpy)
  - [Stage 3: Silver Cleaning (clean.py)](#stage-3-silver-cleaning-cleanpy)
  - [Stage 4: Gold Transformation (transform.py)](#stage-4-gold-transformation-transformpy)
- [Key Data Engineering Concepts](#key-data-engineering-concepts)
- [How to Run](#how-to-run)
- [Technologies Used](#technologies-used)

---

## Architecture Overview

This project follows the **Medallion (Multi-Hop) Architecture**, a layered data-lake pattern that progressively refines data quality at each stage:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MEDALLION ARCHITECTURE                         │
│                                                                       │
│   data/raw/             data/bronze/          data/silver/            │
│   ┌──────────┐         ┌──────────────┐      ┌──────────────┐        │
│   │  9 CSV   │──────►  │  Parquet     │────► │  Parquet     │        │
│   │  Files   │ ingest  │  (schema-    │clean │  (cleaned,   │        │
│   │          │  .py    │   enforced)  │ .py  │   validated) │        │
│   └──────────┘         └──────────────┘      └──────┬───────┘        │
│                                                      │               │
│                                              transform.py            │
│                                                      │               │
│                                               data/gold/             │
│                                              ┌──────────────┐        │
│                                              │  Parquet     │        │
│                                              │  (business-  │        │
│                                              │   ready)     │        │
│                                              └──────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Format | Key Action |
|---|---|---|---|
| **Raw** | Landing zone for source data | CSV | Untouched originals |
| **Bronze** | Schema-enforced copy of raw data | Parquet | Explicit schemas applied, CSV → Parquet conversion |
| **Silver** | Cleaned, validated, enriched data | Parquet | Deduplication, type casting, text normalization, business filtering |
| **Gold** | Analytics-ready aggregated tables | Parquet | Joins, grouping, KPI calculations (Kimball-style dimensional modeling) |

---

## Dataset Description

The source data comes from **Olist**, a Brazilian e-commerce marketplace. It contains ~100k orders made between 2016–2018 across multiple sellers and product categories.

| File | Table Name | Records | Description |
|---|---|---|---|
| `olist_orders_dataset.csv` | orders | ~99,441 | Order-level metadata (status, timestamps) |
| `olist_order_items_dataset.csv` | items | ~112,650 | Line items within each order (product, seller, price, freight) |
| `olist_order_payments_dataset.csv` | payments | ~103,886 | Payment details (type, installments, value) |
| `olist_order_reviews_dataset.csv` | reviews | ~99,224 | Customer reviews (score, comment text) |
| `olist_customers_dataset.csv` | customers | ~99,441 | Customer profiles (city, state, zip code) |
| `olist_sellers_dataset.csv` | sellers | ~3,095 | Seller profiles (city, state, zip code) |
| `olist_products_dataset.csv` | products | ~32,951 | Product catalog (category, dimensions, weight) |
| `olist_geolocation_dataset.csv` | geolocation | ~1,000,163 | Zip-code-level lat/long coordinates |
| `product_category_name_translation.csv` | translation | ~71 | Portuguese → English category name lookup |

---

## Project Structure

```
Ecommerce-pipeline/
├── data/
│   ├── raw/                    # Source CSV files (landing zone)
│   ├── bronze/                 # Schema-enforced Parquet (9 tables)
│   ├── silver/                 # Cleaned Parquet (8 tables)
│   └── gold/                   # Aggregated Parquet (2 tables)
│       ├── daily_revenue/
│       └── seller_performance/
├── src/
│   ├── spark_session.py        # Spark session factory
│   ├── schemas.py              # Explicit StructType schemas for all 9 datasets
│   ├── ingest.py               # Bronze layer: CSV → Parquet ingestion
│   ├── clean.py                # Silver layer: cleaning & validation rules
│   ├── transform.py            # Gold layer: business aggregation & KPIs
│   └── job.py                  # (Placeholder) Pipeline orchestrator
├── tests/                      # Unit tests directory
├── explore.ipynb               # EDA notebook (data profiling & discovery)
├── notes.md                    # Cleaning rules documented during exploration
├── .gitignore
└── README.md
```

---

## Pipeline Stages — Step by Step

### Stage 0: Exploration (`explore.ipynb`)

> **Concept: Exploratory Data Analysis (EDA)**
>
> Before building any pipeline, you need to *understand your data*. EDA is the process of profiling datasets to discover schemas, null patterns, duplicates, and parsing quirks. The findings become the cleaning rules that drive the Silver layer.

The notebook performs the following investigation:

1. **Spark Session Creation** — A `SparkSession` named `"OlistExploration"` is created. This is the entry point for all PySpark operations.

2. **Bulk CSV Loading** — All 9 datasets are loaded from `data/raw/` using `spark.read.csv()` with `header=True` and `inferSchema=True` (schema inference — the lazy approach used only during exploration, replaced with explicit schemas in production).

3. **Schema Inspection** — `printSchema()` reveals data types. The notebook discovers that timestamps are correctly inferred but should be explicitly enforced in production.

4. **Null Value Audit** — A PySpark pattern using `count(when(col(c).isNull(), c))` counts nulls per column. Key findings:
   - `order_approved_at`: **160** nulls (orders not yet approved)
   - `order_delivered_carrier_date`: **1,783** nulls (not yet shipped)
   - `order_delivered_customer_date`: **2,965** nulls (not yet delivered)
   - `product_category_name`: **610** nulls (uncategorized products)

5. **Duplicate Detection** — `groupBy().count().filter("count > 1")` finds:
   - `orders`: **No duplicates** on `order_id` ✅
   - `reviews`: **Duplicates found** on `review_id` — initially caused by a CSV parsing issue with multi-line review text

6. **CSV Multi-Line Fix** — Reviews contain newlines within quoted fields, corrupting row boundaries. Re-reading with `multiLine=True` and `escape='"'` fixes the parsing. Even after the fix, legitimate duplicate `review_id`s remain (one review covering multiple items), which are dropped during cleaning.

7. **Order Status Distribution** — A `groupBy("order_status")` reveals 96,478 delivered orders out of ~99,441 total, confirming that filtering to `is_delivered == True` for revenue calculations is appropriate.

All findings were codified into `notes.md` as formal cleaning rules.

---

### Stage 1: Schema Definition (`schemas.py`)

> **Concept: Explicit Schema Enforcement (Schema-on-Read)**
>
> In production data pipelines, you should *never* rely on `inferSchema=True`. Schema inference scans the entire dataset, is slow, and can guess wrong (e.g., treating a zip code as an integer). Explicit `StructType` schemas guarantee type safety and fail fast if the source data format changes unexpectedly.

This module defines 9 PySpark `StructType` schemas — one for each raw CSV file:

| Schema | Notable Type Decisions |
|---|---|
| `orders_schema` | All timestamps read as `StringType` (cast to `TimestampType` in Silver) |
| `items_schema` | `price` and `freight_value` as `DoubleType`; `order_item_id` as `IntegerType` |
| `payments_schema` | `payment_value` as `DoubleType`; `payment_installments` as `IntegerType` |
| `reviews_schema` | All text fields as `StringType`; dates as `StringType` for later casting |
| `customers_schema` | `customer_zip_code_prefix` as `StringType` (not integer — preserves leading zeros) |
| `sellers_schema` | Same zip-code-as-string approach |
| `products_schema` | Dimension columns (`weight_g`, `length_cm`, etc.) as `IntegerType` |
| `geolocation_schema` | `lat`/`lng` as `DoubleType` for coordinate precision |
| `translation_schema` | Simple two-column lookup table (`StringType` → `StringType`) |

**Why read timestamps as strings first?** Because the raw CSV files contain timestamp strings in a specific format. Reading them as `StringType` at Bronze avoids any parsing failures during ingestion. The `to_timestamp()` cast happens in the Silver layer where we have control over error handling.

---

### Stage 2: Bronze Ingestion (`ingest.py`)

> **Concept: Bronze Layer (Raw Data Preservation)**
>
> The Bronze layer is a faithful, schema-enforced copy of the raw data. Its job is to convert from the source format (CSV) to a columnar, query-optimized format (Parquet) without altering any values. This provides:
> - **Durability**: Parquet is faster and more reliable than CSV for downstream reads
> - **Idempotency**: `mode("overwrite")` means the pipeline can be safely re-run
> - **Schema enforcement**: Data that doesn't match the schema is rejected immediately

**What the code does:**

```python
def ingest_bronze():
    spark = get_spark_session("BronzeIngestion")
    
    # For each dataset:
    spark.read.csv("data/raw/<file>.csv", header=True, schema=<explicit_schema>)
        .write.mode("overwrite").parquet("data/bronze/<table>/")
```

Key implementation details:

| Aspect | Detail |
|---|---|
| **Spark Session** | Obtained via `get_spark_session()` factory — a reusable helper that creates or retrieves the singleton `SparkSession` |
| **Read Options** | `header=True` skips the CSV header row; `schema=<struct>` enforces explicit types |
| **Reviews Special Handling** | Uses `multiLine=True` and `escape='"'` to correctly parse reviews containing newlines and escaped quotes |
| **Write Mode** | `mode("overwrite")` makes the pipeline **idempotent** — running it twice produces the same output |
| **Output Format** | Apache Parquet — a columnar storage format that supports compression, predicate pushdown, and column pruning |

The function writes **9 Parquet directories** under `data/bronze/`:
`orders`, `items`, `payments`, `reviews`, `customers`, `sellers`, `products`, `geolocation`, `translation`

---

### Stage 3: Silver Cleaning (`clean.py`)

> **Concept: Silver Layer (Data Quality & Validation)**
>
> The Silver layer is where raw data becomes *trustworthy*. Cleaning functions apply the rules discovered during EDA: deduplication, type casting, null handling, text normalization, and business-logic filtering. Each table gets its own cleaning function for modularity and testability.

The module defines **8 cleaning functions**, one per Silver table (the `translation` table is consumed during product cleaning and not persisted separately):

#### `clean_orders(spark)`
1. **Read** from `data/bronze/orders/`
2. **Cast 5 timestamp columns** from `StringType` → `TimestampType` using `to_timestamp()`. This enables date arithmetic and time-based filtering downstream.
3. **Add `is_delivered` boolean flag** — a derived column: `True` when `order_status == "delivered"`, `False` otherwise. This avoids repeated string comparisons in the Gold layer and makes the delivery filter explicit.
4. **Write** to `data/silver/orders/`

#### `clean_reviews(spark)`
1. **Drop duplicate reviews** using `dropDuplicates(["review_id"])` — keeps the first occurrence, discards duplicates.
2. **Cast 2 timestamp columns** (`review_creation_date`, `review_answer_timestamp`).
3. **Write** to `data/silver/reviews/`

#### `clean_items(spark)`
1. **Filter out invalid items** — removes rows where `price <= 0` (zero/negative prices are meaningless for revenue).
2. **Cast `shipping_limit_date`** from `StringType` → `TimestampType`.
3. **Write** to `data/silver/items/`

#### `clean_customers(spark)`
1. **Drop duplicate customers** on `customer_id`.
2. **Normalize city names** — `lower(trim(col("customer_city")))` converts `" São Paulo "` → `"são paulo"` for consistent grouping and joining.
3. **Write** to `data/silver/customers/`

#### `clean_products(spark)`
1. **Join with translation table** — `LEFT JOIN` on `product_category_name` to add the `product_category_name_english` column. A left join ensures products without a translation are not lost.
2. **Fill missing categories** — `fillna({"product_category_name_english": "unknown"})` replaces `null` English names with `"unknown"`.
3. **Write** to `data/silver/products/`

#### `clean_payments(spark)`
- **Pass-through** — no cleaning rules applied. Data is written directly from Bronze to Silver to maintain layer completeness.

#### `clean_sellers(spark)`
- **Normalize city names** — `lower(trim(...))` on `seller_city`.

#### `clean_geolocation(spark)`
- **Normalize city names** — `lower(trim(...))` on `geolocation_city`.

---

### Stage 4: Gold Transformation (`transform.py`)

> **Concept: Gold Layer (Business Aggregations & Dimensional Modeling)**
>
> The Gold layer produces analytics-ready tables. This project follows the **Kimball dimensional modeling** approach, creating fact-like aggregated tables from the cleaned Silver data. These tables answer specific business questions and are optimized for dashboarding and reporting.

Two Gold tables are built:

#### `build_daily_revenue(spark)` — Fact Table

**Business Question:** *What was the total revenue per day?*

Steps:
1. **Join** `silver/orders` ↔ `silver/items` on `order_id` (inner join — only items with valid orders).
2. **Filter** to `is_delivered == True` — only count revenue from successfully completed orders. Without this filter, canceled orders would falsely inflate revenue numbers.
3. **Aggregate** by `to_date(order_purchase_timestamp)`:
   - `SUM(price)` → `total_item_revenue` (product revenue)
   - `SUM(freight_value)` → `total_freight_revenue` (shipping revenue)
4. **Order** by `date` ascending.
5. **Write** to `data/gold/daily_revenue/`.

#### `build_seller_performance(spark)` — Dimension + Facts Table

**Business Question:** *How does each seller perform in terms of sales volume, revenue, and customer satisfaction?*

Steps:
1. **Three-way join**:
   - `silver/items` ↔ `silver/orders` (inner join — verify delivery status)
   - ↔ `silver/reviews` (left join — some orders may not have reviews yet)
2. **Filter** to `is_delivered == True`.
3. **Aggregate** by `seller_id`:
   - `COUNT(order_item_id)` → `total_items_sold`
   - `SUM(price)` → `total_revenue`
   - `ROUND(AVG(review_score), 2)` → `avg_review_score`
4. **Order** by `total_revenue` descending (top sellers first).
5. **Write** to `data/gold/seller_performance/`.

---

## Key Data Engineering Concepts

| Concept | Where It Appears | Explanation |
|---|---|---|
| **Medallion Architecture** | Entire pipeline | A layered data-lake pattern (Bronze/Silver/Gold) that progressively refines data quality |
| **Schema-on-Read** | `schemas.py` → `ingest.py` | Enforcing explicit schemas at read time rather than relying on inference |
| **Parquet Format** | All layers after Raw | A columnar storage format optimized for analytics workloads — supports compression, predicate pushdown, and column pruning |
| **Idempotency** | `write.mode("overwrite")` | The pipeline can be re-run safely without duplicating data |
| **EDA-Driven Cleaning** | `explore.ipynb` → `notes.md` → `clean.py` | Cleaning rules derived from systematic data profiling, not guesswork |
| **CSV Multi-Line Handling** | `ingest.py` (reviews) | Using `multiLine=True` and `escape='"'` to correctly parse CSVs with embedded newlines |
| **Deduplication** | `clean.py` | `dropDuplicates()` removes redundant rows by primary key |
| **Type Casting** | `clean.py` | Converting string timestamps to `TimestampType` for proper date operations |
| **Text Normalization** | `clean.py` | `lower(trim(...))` standardizes free-text fields for consistent joins and grouping |
| **Derived Columns** | `clean.py` (`is_delivered`) | Computing boolean flags from business logic to simplify downstream queries |
| **Kimball Dimensional Modeling** | `transform.py` | Structuring Gold tables as fact/dimension tables for analytics |
| **Left vs Inner Joins** | `transform.py`, `clean.py` | Inner joins for strict matching; left joins to preserve all rows from the left table |
| **Business Logic Filtering** | `transform.py` | Excluding non-delivered orders from revenue to prevent metric inflation |

---

## How to Run

### Prerequisites

- **Python 3.10+**
- **Java 11+** (required by Spark)
- **Apache Spark 3.x** / PySpark

### Setup

```bash
# 1. Clone the repository
git clone <repository-url>
cd Ecommerce-pipeline

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install PySpark
pip install pyspark

# 4. Download the Olist dataset from Kaggle and place CSVs in data/raw/
#    https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
```

### Run the Pipeline

```bash
# Step 1: Ingest Raw → Bronze
cd src
python ingest.py

# Step 2: Clean Bronze → Silver
python clean.py

# Step 3: Transform Silver → Gold
python transform.py
```

Each step prints a confirmation message upon completion (e.g., `"Bronze ingestion complete!"`).

---

## Technologies Used

| Technology | Purpose |
|---|---|
| **Python 3.14** | Primary programming language |
| **Apache Spark / PySpark** | Distributed data processing engine |
| **Apache Parquet** | Columnar storage format for all pipeline layers |
| **Jupyter Notebook** | Interactive exploratory data analysis |
| **Git** | Version control |
