# Open Food Facts Analytics Pipeline

A production-grade data engineering pipeline ingesting live product data from the Open Food Facts API into a cloud-hosted star schema, orchestrated with Apache Airflow.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29b5e8)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017cee)
![dbt](https://img.shields.io/badge/dbt-1.x-FF694B)
![pytest](https://img.shields.io/badge/pytest-passing-green)

---

## Overview

This project builds an end-to-end analytics pipeline on the [Open Food Facts](https://world.openfoodfacts.org) dataset — a free, real REST API with 3M+ food products from 180+ countries, no API key required.

The pipeline ingests live product data, stages it in AWS S3 (medallion architecture), loads it into Snowflake via bulk COPY, models it into a star schema using dbt, and delivers insights through an interactive dashboard.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.10 |
| Containerization | Docker + Compose |
| Data Lake | AWS S3 |
| Data Warehouse | Snowflake |
| Ingestion / Transform | Python 3.12, Pandas |
| Data Modeling | dbt |
| Testing | pytest + dbt test |
| Visualization | Power BI Desktop |

---

## Architecture

```
Open Food Facts API  (world.openfoodfacts.org/api/v2)
      │
      ▼
pipeline/extract.py     →  S3: raw/products/YYYY-MM-DD/       (JSON)
      │
      ▼
pipeline/validate.py    →  S3: staged/products/YYYY-MM-DD/    (JSON, validated)
      │
      ▼
pipeline/transform.py   →  S3: processed/products/YYYY-MM-DD/ (Parquet)
      │
      ▼
pipeline/load.py        →  Snowflake: stg_products             (COPY from S3)
      │
      ▼
dbt run                 →  Snowflake: fact_products + 4 dims   (star schema)
      │
      ▼
Power BI Dashboard
```

---

## Setup

### Prerequisites
- Python 3.12
- Docker Desktop
- AWS account (S3 bucket)
- Snowflake account (free trial)

### 1. Clone the repo
```bash
git clone https://github.com/morganmicah200/openfoodfacts-pipeline.git
cd openfoodfacts-pipeline
```

### 2. Create virtual environment
```bash
py -3.12 -m venv .venv
.venv\Scripts\activate       # Windows
# source .venv/bin/activate  # Mac/Linux
pip install -r requirements.txt
```

### 3. Configure environment variables
```bash
cp .env.example .env
# Fill in your AWS and Snowflake credentials in .env
```

### 4. Start Airflow
```bash
docker compose up airflow-init
docker compose up -d
```
Airflow UI available at `http://localhost:8080` (admin / admin)

### 5. Run manually (without Airflow)
```bash
python main.py
```

---

## Project Structure

```
openfoodfacts-pipeline/
├── pipeline/
│   ├── extract.py          # OFF API calls, pagination, S3 raw write
│   ├── validate.py         # Schema + null checks, S3 staged write
│   ├── transform.py        # Type casting, normalize, S3 Parquet write
│   └── load.py             # Snowflake COPY from S3 external stage
├── dags/
│   └── off_pipeline.py     # Airflow DAG — 6 tasks, daily schedule
├── off_dbt/
│   └── models/
│       ├── staging/
│       │   └── stg_products_clean.sql
│       └── marts/
│           ├── fact_products.sql
│           ├── dim_brand.sql
│           ├── dim_category.sql
│           ├── dim_country.sql
│           └── dim_nutrition_grade.sql
├── sql/
│   └── analysis_queries.sql
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── docs/
│   └── architecture.md
├── docker-compose.yml
├── config.py
├── main.py
└── requirements.txt
```

---

## Data Source

[Open Food Facts](https://world.openfoodfacts.org) — Free, open, crowdsourced food product database. No API key required. 3M+ products, 180+ countries.

---

## Author

Micah Morgan — [GitHub](https://github.com/morganmicah200)