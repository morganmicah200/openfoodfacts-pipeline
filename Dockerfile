FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt