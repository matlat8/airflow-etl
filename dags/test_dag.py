from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "start_date": "2020-05-13",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag_id = "SomethingDag"
with DAG(dag_id=dag_id, default_args=default_args, schedule_interval="@daily") as dag:
    dummy_task = DummyOperator(task_id="dummy_task")
    something = PythonOperator(task_id="something", python_callable=lambda: print("Something"))
    dummy_task