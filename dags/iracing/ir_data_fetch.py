from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from b2_upload import BackblazeB2UploadOperator

default_args = {
    "owner": "airflow",
    "start_date": "2020-05-13",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def do_the_thing():
    print("Doing the thing")

dag_id = "IrDataFetchDag"
with DAG(dag_id=dag_id, default_args=default_args, schedule_interval="@daily") as dag:
    something = PythonOperator(task_id="something", python_callable=do_the_thing)
    
    something