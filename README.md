# TMDB Movie Data Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29b5e8)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017cee)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED)

---

## Overview

End-to-end data engineering pipeline that ingests movie data from the TMDB API, stages it in AWS S3, loads it into Snowflake, and models it into a star schema using dbt. Orchestrated daily with Apache Airflow running in Docker. Initial backfill loaded **1,047,481 movies** — the pipeline now runs incrementally every 24 hours, appending new and updated movies using the TMDB changes endpoint.

---

## Architecture
```
TMDB API → extract.py → S3 raw/movies/YYYY-MM-DD/batch_XXXX.json
         → validate.py → S3 staged/movies/YYYY-MM-DD/batch_XXXX.json
         → transform.py → S3 processed/movies/YYYY-MM-DD/movies.parquet
         → load.py → Snowflake TMDB.RAW.stg_movies (COPY INTO)
         → dbt → TMDB.ANALYTICS.fact_movies + 4 dims
```

**Medallion architecture:** raw → staged → processed → analytics

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Pipeline development |
| aiohttp / asyncio | Async concurrent API extraction |
| AWS S3 | Cloud object storage (medallion layers) |
| Apache Parquet | Columnar format for Snowflake ingestion |
| Snowflake | Cloud data warehouse |
| dbt | Dimensional modeling, testing, documentation |
| Apache Airflow 2.10 | Orchestration and scheduling |
| Docker | Containerized pipeline (separate Airflow + dbt containers) |
| pytest | Unit testing |

---

## Pipeline Stages

### Extract
Downloads the full TMDB movie catalog using the daily ID export file, then fetches full movie details concurrently using `aiohttp` with 50 simultaneous requests and an `asyncio` semaphore for rate limiting. Checkpointed to S3 after every batch so extraction can resume if interrupted.

For daily runs, the TMDB changes endpoint is used to fetch only movies added or modified in the last 24 hours — typically a few hundred to a few thousand movies per day.

### Validate
Validates each record for required fields (`id`, `title`, `release_date`), correct data types, valid ranges (`vote_average` 0–10, non-negative `runtime`), and cross-batch duplicate detection using a `seen_ids` set. Passing records are written to the staged S3 prefix.

### Transform
Flattens nested JSON (genres, languages, production companies) into pipe-delimited strings and writes a single Parquet file to the processed S3 prefix using PyArrow.

### Load
Uses Snowflake's `COPY INTO` command to bulk load the Parquet file from S3 into `TMDB.RAW.stg_movies`. New records are appended on each daily run — historical data is preserved.

### dbt
Builds a star schema in `TMDB.ANALYTICS` on top of `stg_movies`. All models tested with dbt's built-in testing framework.

---

## Data

| Metric | Value |
|--------|-------|
| Total movie IDs in TMDB export | 1,168,284 |
| Movies extracted and loaded | 1,047,481 |
| Failed validation | 120,803 |
| Primary failure reason | Missing `release_date` (99.9%) |
| Duplicate IDs caught | 4 |
| S3 batch files | 117 |
| dbt tests passing | 14/14 |

---

## Snowflake Schema

### TMDB.RAW
- `stg_movies` — flat staging table loaded via COPY INTO from Parquet

### TMDB.ANALYTICS (built by dbt)
| Table | Rows |
|-------|------|
| `fact_movies` | 1,047,481 |
| `dim_genre` | 19 |
| `dim_language` | 178 |
| `dim_release_date` | 44,099 |
| `dim_production_company` | 199,905 |

---

## Airflow DAG

The `tmdb_pipeline` DAG runs daily at 06:00 UTC. On each run:
- **Extract** — fetches changed/new movies from the TMDB changes endpoint
- **Validate** — quality checks and deduplication
- **Transform** — flattens JSON to Parquet
- **Load** — appends new rows to Snowflake via COPY INTO
- **dbt run** — rebuilds star schema including new records

![DAG Graph](images/Airflow_graph.png)
![Task Duration](images/Airflow_Chart.png)
![Airflow Bars](images/Airflow_bars.png)

---

## Docker

Two containers:
- **Airflow** — webserver + scheduler running the DAG
- **dbt** — lightweight `python:3.12-slim` container that executes `dbt run` when called via `docker exec` from the Airflow BashOperator

---

## How to Run
```bash
# Clone the repo
git clone https://github.com/morganmicah200/tmdb-pipeline.git
cd tmdb-pipeline

# Set up environment
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Fill in TMDB_API_KEY, AWS credentials, Snowflake credentials

# Start Docker
docker-compose up -d

# Trigger pipeline
# Open http://localhost:8080 — admin/admin
# Toggle tmdb_pipeline DAG on and trigger manually
```

---

## Author

Micah Morgan — [GitHub](https://github.com/morganmicah200)