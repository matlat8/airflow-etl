from datetime import timedelta, datetime
from iracingdataapi.client import irDataClient
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
import json
import io
import os

py_file_path = os.path.dirname(os.path.abspath(__file__))

default_args = {
    "owner": "airflow",
    "start_date": "2024-12-18",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
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

def get_ir_client():
    conn = BaseHook.get_connection("iracing")
    return irDataClient(username=conn.login, password=conn.password)

dag_id = "IRConstantsFetchDag"
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         catchup=False,
         schedule_interval='0 1 * * *',
         tags=['iRacing', 'Extraction'],
         max_active_runs=1
         ) as dag:
    
    with TaskGroup(group_id='cars') as cars:
        @task()
        def get_cars():
            s3 = BotoConnection.get_instance()
            ir = get_ir_client()
            cars = ir.cars

            json_data = json.dumps(cars)
            bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
            s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/cars.json')
            return cars

        @task()
        def create_cars_staging_table():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'cars/create_cars_stg_table.sql'), 'r') as f:
                query = f.read()

            db.query(query)

        @task()
        def load_cars_into_staging():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            s3_creds = BaseHook.get_connection("backblaze_b2_default")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'cars/load_cars.sql'), 'r') as f:
                query = f.read().format(
                    filepath=f'{s3_creds.host}/{s3_creds.schema}/STG/iRacing/cars.json',
                    access_key=s3_creds.login,
                    secret_key=s3_creds.password
                )

            db.query(query)

        @task()
        def version_cars_into_final():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'cars/version_cars_into_final.sql'), 'r') as f:
                query = f.read()

            db.query(query)


        cars = get_cars()
        cars_stging = create_cars_staging_table()
        cars_loaded_into_staging = load_cars_into_staging()
        cars_versioned = version_cars_into_final()
    
    cars >> cars_stging >> cars_loaded_into_staging >> cars_versioned
    
    ####################
    #      Tracks      #
    ####################
    
    with TaskGroup(group_id='tracks') as tracks:
        
        @task()
        def get_tracks():
            s3 = BotoConnection.get_instance()
            ir = get_ir_client()
            tracks = ir.tracks

            json_data = json.dumps(tracks)
            bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
            s3.Bucket('ML-Clickhouse').upload_fileobj(bytes_buffer, f'STG/iRacing/tracks.json')
            return tracks
        
        @task()
        def create_tracks_staging_table():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'tracks/create_tracks_stg_table.sql'), 'r') as f:
                query = f.read()

            db.query(query)
            
        @task()
        def load_tracks_into_staging():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            s3_creds = BaseHook.get_connection("backblaze_b2_default")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'tracks/load_tracks.sql'), 'r') as f:
                query = f.read().format(
                    filepath=f'{s3_creds.host}/{s3_creds.schema}/STG/iRacing/tracks.json',
                    access_key=s3_creds.login,
                    secret_key=s3_creds.password
                )

            db.query(query)
            
        @task()
        def version_tracks_into_final():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'tracks/version_tracks_into_final.sql'), 'r') as f:
                query = f.read()

            db.query(query)
            
        tracks = get_tracks()
        tracks_stging = create_tracks_staging_table()
        tracks_loaded_into_staging = load_tracks_into_staging()
        tracks_versioned = version_tracks_into_final()
        
        tracks >> tracks_stging >> tracks_loaded_into_staging >> tracks_versioned
    
    cars
    tracks