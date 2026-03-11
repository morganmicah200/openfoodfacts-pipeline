"""
dags/off_pipeline.py
Airflow DAG for the Open Food Facts analytics pipeline.
Schedule: daily at 06:00 UTC
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "micah",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

@dag(
    dag_id="off_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openfoodfacts", "s3", "snowflake"],
    description="Ingest Open Food Facts API → S3 → Snowflake → dbt",
)
def off_pipeline():

    # ── Extract ────────────────────────────────────────────────────────────
    extract = PythonOperator(
        task_id="extract_products",
        python_callable=lambda: None,   # TODO: replace with extract()
    )

    # ── Validate ───────────────────────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_products",
        python_callable=lambda: None,   # TODO: replace with validate()
    )

    # ── Transform ──────────────────────────────────────────────────────────
    transform = PythonOperator(
        task_id="transform_products",
        python_callable=lambda: None,   # TODO: replace with transform()
    )

    # ── Load ───────────────────────────────────────────────────────────────
    load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=lambda: None,   # TODO: replace with load()
    )

    # ── dbt run ────────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/off_dbt && dbt run --profiles-dir .",
    )

    # ── dbt test ───────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/off_dbt && dbt test --profiles-dir .",
    )

    # ── Task Dependencies ──────────────────────────────────────────────────
    extract >> validate >> transform >> load >> dbt_run >> dbt_test


off_pipeline()