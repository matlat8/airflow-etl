from datetime import timedelta, datetime
from iracingdataapi.client import irDataClient
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": "2024-12-18",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

def get_ir_client():
    conn = BaseHook.get_connection("iracing")
    return irDataClient(username=conn.login, password=conn.password)

class BotoConnection:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        import boto3
        if cls._instance is None:
            connection_id = "backblaze_b2_default"
            connection = BaseHook.get_connection(connection_id)
            
            if not connection:
                print(f"No Airflow connection found with Conn Id '{connection_id}'.")
                raise ValueError(f"Airflow connection '{connection_id}' not found.")
            s3 = boto3.resource(
                's3',
                aws_access_key_id=connection.login,
                aws_secret_access_key=connection.password,
                endpoint_url=connection.host
            )
            cls._instance = s3
        return cls._instance

dag_id = "IrDataFetchIncremental"
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         schedule_interval='@hourly',
         catchup=False
         ) as dag:
    @task()
    def get_latest_endtime():
        import clickhouse_connect
        
        db_conn = BaseHook.get_connection("clickhouse_prod")
        client = clickhouse_connect.get_client(host=db_conn.host, username=db_conn.login, password=db_conn.password, port=db_conn.port)
        
        result = client.query("SELECT max(end_time) FROM iracing.series")
        return result.result_rows[0][0]
        

    start_time = get_latest_endtime()
    
    run_data_pull_dag = TriggerDagRunOperator(
        task_id='run_data_pull_dag',
        trigger_dag_id='IrDataFetchDag',  # Replace with the ID of the DAG you want to trigger
        conf={"start_time": "{{ task_instance.xcom_pull(task_ids='get_latest_endtime') }}"},
        wait_for_completion=True,
    )
    
    start_time >> run_data_pull_dag