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



---

## Data Source

[Open Food Facts](https://world.openfoodfacts.org) — Free, open, crowdsourced food product database. No API key required. 3M+ products, 180+ countries.

---

## Author

Micah Morgan — [GitHub](https://github.com/morganmicah200)