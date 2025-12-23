from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
import os

def show():
    print("ENV AIRFLOW__API__SECRET_KEY =", os.getenv("AIRFLOW__API__SECRET_KEY"))
    print("CONF api.secret_key =", conf.get("api", "secret_key", fallback=None))
    print("ENV AIRFLOW__EXECUTION_API__BASE_URL =", os.getenv("AIRFLOW__EXECUTION_API__BASE_URL"))
    print("CONF execution_api.base_url =", conf.get("execution_api", "base_url", fallback=None))

with DAG(
    dag_id="debug_exec_api",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="show", python_callable=show)
