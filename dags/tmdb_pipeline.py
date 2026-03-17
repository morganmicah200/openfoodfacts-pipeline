from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from pipeline.extract import run_incremental_extract_sync
from pipeline.validate import run_validate
from pipeline.transform import run_transform
from pipeline.load import run_load

default_args = {
    "owner": "micah",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="tmdb_pipeline",
    default_args=default_args,
    description="Daily TMDB movie pipeline: incremental extract → validate → transform → load → dbt",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 9),
    catchup=False,
    tags=["tmdb", "movies"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=run_incremental_extract_sync,
        op_kwargs={"source_date": "{{ ds }}"},
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=run_validate,
        op_kwargs={"source_date": "{{ ds }}"},
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
        op_kwargs={"source_date": "{{ ds }}"},
    )

    load = PythonOperator(
        task_id="load",
        python_callable=run_load,
        op_kwargs={"source_date": "{{ ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec tmdb-pipeline-dbt-1 dbt run --profiles-dir /opt/dbt",
    )

    extract >> validate >> transform >> load >> dbt_run