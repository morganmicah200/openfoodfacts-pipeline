# Pipeline Architecture

## Overview

This pipeline ingests movie data from the TMDB API into a Snowflake analytics star schema. It follows a medallion architecture (raw → staged → processed → analytics) with each layer stored in AWS S3 before being loaded into Snowflake. Apache Airflow orchestrates the daily incremental runs via a Dockerized DAG.

---

## Data Flow
```
TMDB API
    │
    ▼
extract.py — fetches movie details concurrently (aiohttp, asyncio, semaphore)
    │
    ▼
S3: raw/movies/YYYY-MM-DD/batch_XXXX.json
    │
    ▼
validate.py — quality checks, deduplication, type validation
    │
    ▼
S3: staged/movies/YYYY-MM-DD/batch_XXXX.json
    │
    ▼
transform.py — flattens nested JSON, writes columnar Parquet
    │
    ▼
S3: processed/movies/YYYY-MM-DD/movies.parquet
    │
    ▼
load.py — Snowflake COPY INTO (bulk load from S3)
    │
    ▼
Snowflake: TMDB.RAW.stg_movies
    │
    ▼
dbt — dimensional modeling, testing
    │
    ▼
Snowflake: TMDB.ANALYTICS
    ├── fact_movies (1,060,855 rows)
    ├── dim_genre (19 rows)
    ├── dim_language (179 rows)
    ├── dim_release_date (44,102 rows)
    └── dim_production_company (200,091 rows)
```

---

## Extraction Strategy

### Historical Backfill (one-time)
On initial load, `extract.py` downloaded the full TMDB movie ID export (~1.2M IDs), fetched full movie details for each via the TMDB API, and wrote them to S3 in batches of 10,000. Checkpointing after each batch allowed the extract to resume if interrupted. Total: **1,047,481 movies** loaded on 2026-03-09.

### Incremental (daily)
On each scheduled run, `run_incremental_extract` hits the TMDB `/movie/changes` endpoint to retrieve only movies added or modified in the last 24 hours — typically a few hundred to a few thousand movies per day. These are fetched concurrently, validated, transformed, and appended to `stg_movies` without touching historical data.

---

## Concurrency

The extract uses Python's `asyncio` with `aiohttp` to fire 50 simultaneous API requests via a `Semaphore(50)`. This reduced the full backfill from an estimated ~72 hours (sequential) to a few hours.

---

## Medallion Layers

| Layer | Location | Format | Purpose |
|-------|----------|--------|---------|
| Raw | `s3://bucket/raw/movies/YYYY-MM-DD/` | JSON | Unmodified API responses |
| Staged | `s3://bucket/staged/movies/YYYY-MM-DD/` | JSON | Validated, deduplicated records |
| Processed | `s3://bucket/processed/movies/YYYY-MM-DD/` | Parquet | Flattened, typed, ready for Snowflake |
| Analytics | `TMDB.ANALYTICS` | Snowflake tables | Star schema for reporting |

---

## Star Schema

`fact_movies` is the central fact table joined to four dimension tables. Movies with multiple genres, languages, or production companies are linked to their primary value — a bridge table would be required for full many-to-many modeling.

---

## Orchestration

The `tmdb_pipeline` Airflow DAG runs daily at 06:00 UTC with the following task chain:
```
extract → validate → transform → load → dbt_run
```

Airflow and dbt run in separate Docker containers. The `dbt_run` task uses a `BashOperator` to call `docker exec` into the dbt container, keeping dbt dependencies isolated from the Airflow environment.

---

## Infrastructure

| Component | Detail |
|-----------|--------|
| S3 bucket | `openfoodfacts-pipeline-micah` (us-east-2) |
| Snowflake account | `wmxybwc-sl89298` (AWS us-east-1) |
| Snowflake warehouse | `COMPUTE_WH` (auto-suspend 60s) |
| Airflow | 2.10.2 (Docker, LocalExecutor) |
| dbt | 1.11.3 (Snowflake adapter) |