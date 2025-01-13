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
import requests

def get_schedules(obj) -> list[dict]:
    return obj['schedules']

def get_weather(obj) -> dict:
    return obj['weather']

def upload_json_file(s3, data, bucket, key):
    json_data = json.dumps(data)
    bytes_buffer = io.BytesIO(json_data.encode('utf-8'))
    s3.Bucket(bucket).upload_fileobj(bytes_buffer, key)

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
        
    with TaskGroup(group_id='series_seasons') as series_seasons:
        
        @task()
        def get_series_seasons():
            s3 = BotoConnection.get_instance()
            ir = get_ir_client()
            series_seasons = ir.series_seasons()
            
            return series_seasons
        
        @task()
        def process_one_season(series_season):
            schedules = get_schedules(series_season)
            
            weather_objects = []
            weather_summary_objects = []
            weather_forecast_options_objects = []
            weather_forecast_overtime_objects = []
            for sched in schedules:
                fk_hash = hash(f'{sched["season_id"]}:{sched["race_week_num"]}')
                
                weather = get_weather(sched)
                weather_summary = weather.get('weather_summary')
                weather_forecast_options = weather.get('forecast_options')
                print(weather)
                
                # Get the nested weather overtime url
                weather_forecast_url = weather.get('weather_url')
                if weather_forecast_url is not None:
                    weather_forecast_overtime = requests.get(weather_forecast_url).json()
                
                # Apply Pk to base data so we can join them later
                sched['key_series_season_schedule'] = fk_hash
                
                # Apply Fk to broken out data to join on
                if weather_summary is not None:
                    weather_summary['key_series_season_schedule'] = fk_hash
                if weather_forecast_options is not None:
                    weather_forecast_options['key_series_season_schedule'] = fk_hash
                if weather_forecast_url is not None and weather_forecast_overtime is not None:
                    for forecast in weather_forecast_overtime:
                        print(forecast)
                        forecast['key_series_season_schedule'] = fk_hash
                
                weather['key_series_season_schedule'] = fk_hash
                
                # Remove attributes from main data that are now in the broken out data
                sched.pop('weather')
                
                for key in ('weather_summary', 'forecast_options', 'weather_url'):
                    if key in weather:
                        weather.pop(key)
                
                # Append to lists
                weather_objects.append(weather)
                weather_summary_objects.append(weather_summary)
                weather_forecast_options_objects.append(weather_forecast_options)
                if weather_forecast_url is not None:
                    weather_forecast_overtime_objects.append(weather_forecast_overtime)
                
            # Remove schedules since we chunk it out to its own json dict
            series_season.pop('schedules')
            
            # Upload to S3
            s3 = BotoConnection.get_instance()
            upload_json_file(s3, schedules, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-schedules.json')
            upload_json_file(s3, weather_objects, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-weather.json')
            upload_json_file(s3, weather_summary_objects, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-weather_summary.json')
            upload_json_file(s3, weather_forecast_options_objects, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-weather_forecast_options.json')
            upload_json_file(s3, series_season, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-series_season.json')
            upload_json_file(s3, weather_forecast_overtime_objects, 'ML-Clickhouse', f'STG/iRacing/series_seasons/{series_season["season_id"]}-weather_forecast_overtime.json')
            
            return
        
        @task()
        def create_series_season_stg():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/series_season_create_stg.sql'), 'r') as f:
                query = f.read()

            db.query(query)
            
        @task()
        def load_series_season_stg():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            s3_creds = BaseHook.get_connection("backblaze_b2_default")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/load_stg.sql'), 'r') as f:
                query = f.read().format(
                    key=f'{s3_creds.host}/{s3_creds.schema}/STG/iRacing/series_seasons/*-series_season.json',
                    access_key=s3_creds.login,
                    secret_key=s3_creds.password,
                    tablename='stg_series_season'
                )

            db.query(query)
            
        @task()
        def version_series_season():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/version_data.sql'), 'r') as f:
                query = f.read().format(
                    target_table='series_season',
                    stage_table='stg_series_season'
                )

            db.query(query)
            
        seasons = process_one_season.expand(series_season=get_series_seasons())
        series_seasons_stg = create_series_season_stg()
        stg_series_season_loaded = load_series_season_stg()
        series_season_versioned = version_series_season()
        
        seasons >> series_seasons_stg >> stg_series_season_loaded >> series_season_versioned
            
    with TaskGroup(group_id='series_seasons_schedules') as series_seasons_schedules:
        @task()
        def create_series_seasons_schedules_stg():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/stg_series_seasons_schedules.sql'), 'r') as f:
                query = f.read()

            db.query(query)
            
        createSeriesSeasonsSchedules = create_series_seasons_schedules_stg()
        
        @task()
        def load_series_seasons_schedules_stg():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            s3_creds = BaseHook.get_connection("backblaze_b2_default")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/load_stg.sql'), 'r') as f:
                query = f.read().format(
                    key=f'{s3_creds.host}/{s3_creds.schema}/STG/iRacing/series_seasons/*-schedules.json',
                    access_key=s3_creds.login,
                    secret_key=s3_creds.password,
                    tablename='stg_series_seasons_schedules'
                )

            db.query(query)
            
        loadSeriesSeasonsSchedulesStg = load_series_seasons_schedules_stg()
        
        @task()
        def version_series_seasons_schedules():
            import clickhouse_connect

            db_creds = BaseHook.get_connection("clickhouse_prod")
            db = clickhouse_connect.get_client(host=db_creds.host, username=db_creds.login, password=db_creds.password, port=db_creds.port)

            with open(os.path.join(py_file_path, 'series_season/version_data.sql'), 'r') as f:
                query = f.read().format(
                    target_table='series_seasons_schedules',
                    stage_table='stg_series_seasons_schedules'
                )

            db.query(query)
            
        versionSeriesSeasonsSchedules = version_series_seasons_schedules()
        
        createSeriesSeasonsSchedules >> loadSeriesSeasonsSchedulesStg >> versionSeriesSeasonsSchedules
    
    cars >> series_seasons
    tracks >> series_seasons
    
    series_seasons >> series_seasons_schedules
    