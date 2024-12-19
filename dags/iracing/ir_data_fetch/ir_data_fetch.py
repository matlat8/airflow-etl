from datetime import timedelta, datetime
from iracingdataapi.client import irDataClient
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
import json
import io

default_args = {
    "owner": "airflow",
    "start_date": "2024-12-18",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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

dag_id = "IrDataFetchDag"
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         params={'start_time': Param(default=datetime.now())},
         schedule_interval=None,
         catchup=False,
         tags=['iRacing', 'Extraction']
         ) as dag:
        
    @task()
    def fetch_ir_data(start_time):
        idc = get_ir_client()
        s3 = BotoConnection.get_instance()
        print(start_time.strftime('%Y-%m-%dT%H:%MZ'))
        search_series_data = idc.result_search_series(finish_range_begin=start_time.strftime('%Y-%m-%dT%H:%MZ'))
        
        json_data = json.dumps(search_series_data)
        bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
        s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/series/{start_time.strftime("%Y-%m-%d_%H-%M-%S")}-searchseries.json')
        
        results = []
        result_lap_chart_data = []
        result_lap_data = []
        result_event_log = []
        for item in search_series_data:
            try: results.append(idc.result(subsession_id=item['subsession_id']))
            except: continue
            #result_lap_data.extend(idc.result_lap_data(subsession_id=item['subsession_id']))
            try: 
                data = idc.result_lap_chart_data(subsession_id=item['subsession_id'])
                for entry in data:
                    entry['subsession_id'] = item['subsession_id']
                result_lap_chart_data.extend(data)
            except: continue
            try: result_event_log.extend(idc.result_event_log(subsession_id=item['subsession_id']))
            except: continue
        print("Fetching iRacing data")
        
        json_data = json.dumps(results)
        bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
        s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/results/{start_time.strftime("%Y-%m-%d_%H-%M-%S")}-results.json')

        json_data = json.dumps(result_lap_chart_data)
        bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
        s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/results_lap_chart_data/{start_time.strftime("%Y-%m-%d_%H-%M-%S")}-results_lap_chart_data.json')

        #json_data = json.dumps(result_lap_data)
        #bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
        #s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/result_lap_data/{start.format("YYYY-MM-DD_HH-mm-ss")}-result_lap_data.json')

        json_data = json.dumps(result_event_log)
        bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
        s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/result_event_log/{start_time.strftime("%Y-%m-%d_%H-%M-%S")}-result_event_log.json')
    
    ir_data = fetch_ir_data(dag.params['start_time'])
    
    ir_data