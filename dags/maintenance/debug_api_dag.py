from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
import os

def show():
    print("ENV AIRFLOW__API__SECRET_KEY =", os.getenv("AIRFLOW__API__SECRET_KEY"))
    print("CONF api.secret_key =", conf.get("api", "secret_key", fallback=None))
    print("ENV AIRFLOW__CORE__EXECUTION_API_SERVER_URL =", os.getenv("AIRFLOW__CORE__EXECUTION_API_SERVER_URL"))
    print("CONF core.execution_api_server_url =", conf.get("core", "execution_api_server_url", fallback=None))

with DAG(
    dag_id="debug_exec_api",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="show", python_callable=show)
