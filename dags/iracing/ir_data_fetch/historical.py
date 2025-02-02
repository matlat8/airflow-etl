from datetime import timedelta, datetime
from iracingdataapi.client import irDataClient
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator

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
    


dag_id = "IrDataFetchHistorical"
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         schedule_interval=None,
         params={'starting_point': Param(default='2024-12-01 00:00:00.000000+00:00'), 'ending_point': Param(default='2025-01-01 00:00:00.000000+00:00')},
         catchup=False,
         tags=['iRacing', 'Historical', 'ETL']
         ) as dag:
    
    @task()
    def get_history_periods(**kwargs):
        dag_run = kwargs.get('dag_run')
        start_date_str = dag_run.conf.get('starting_point', dag.params['starting_point'])
        end_date_str = dag_run.conf.get('ending_point', dag.params['ending_point'])
        
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S.%f%z")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S.%f%z")
        hours = int((end_date - start_date).total_seconds() / 3600)
        history_periods = []
        
        for i in range(hours):
            period = start_date + timedelta(hours=i)
            history_periods.append(period)
            
        history_periods.sort(reverse=True)
        
        return history_periods
    
    def trigger_dag_run(**kwargs):
        history_periods = kwargs['ti'].xcom_pull(task_ids='get_history_periods')
        for period in history_periods:
            trigger = TriggerDagRunOperator(
                task_id=f'trigger_data_pull_dag_{period.strftime("%Y-%m-%d%H%M%S")}',
                trigger_dag_id='IrDataFetchDag',
                conf={"start_time": period.isoformat(), "end_time": (period + timedelta(hours=1)).isoformat()},
                wait_for_completion=True,
            )
            trigger.execute(context=kwargs)

    start_time = get_history_periods()
    
    trigger_dag_runs = PythonOperator(
        task_id='trigger_dag_runs',
        python_callable=trigger_dag_run,
        provide_context=True,
    )
    
    start_time >> trigger_dag_runs