from datetime import timedelta
import json
import uuid
import os
from typing import List

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable

from b2sdk.v2 import InMemoryAccountInfo, B2Api, AuthInfoCache

default_args = {
    "owner": "airflow",
    "start_date": "2024-12-08",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}
class B2Connection:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            connection_id = "backblaze_b2_default"
            connection = BaseHook.get_connection(connection_id)
            
            info = InMemoryAccountInfo()
            b2_api = B2Api(info, cache=AuthInfoCache(info))
            if not connection:
                print(f"No Airflow connection found with Conn Id '{connection_id}'.")
                raise ValueError(f"Airflow connection '{connection_id}' not found.")
            b2_api.authorize_account(
                "production",
                connection.login,  # account_id
                connection.password  # application_key
            )
            cls._instance = b2_api
        return cls._instance
    
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

def find_files_to_process(directory='STG/iRacing/results/'):
    from airflow.models import Variable
    
    # Ensure the directory ends with a slash
    if not directory.endswith('/'):
        directory += '/'
    
    # Get the b2 bucket
    bucket_name = Variable.get('B2_BUCKET')
    bucket = B2Connection.get_instance().get_bucket_by_name(bucket_name)
    
    # List files in the directory        
    files = [
        file_version_info.file_name 
        for file_version_info, folder_name in bucket.ls(directory)
        if file_version_info.file_name.startswith(directory) and '/' not in file_version_info.file_name[len(directory):]
    ]
    
        
    return files
    
dag_id = "IrVersionResults"
dag_timeout = timedelta(hours=1)
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         schedule_interval=None,
         tags=['iRacing', 'ETL']
         ) as dag:
    
    @task
    def find_raw_files():
        return find_files_to_process()
    
    @task
    def find_session_results_files():
        return find_files_to_process(directory='STG/iRacing/results/session_results/')
    
    @task
    def find_results_files():
        return find_files_to_process(directory='STG/iRacing/results/results/')
    
    @task
    def pre_process_file_task(file):
        from airflow.models import Variable
        filename = file.split("/")[-1]
        filepath = file.split("/")[:-1]
        
        # initialize B2 API & download file
        bucket_name = Variable.get('B2_BUCKET')
        bucket = B2Connection.get_instance().get_bucket_by_name(bucket_name)
        downloaded_file = bucket.download_file_by_name(file)
        downloaded_file.save_to(f'./{filename}')
        
        # Open the file and read the data
        with open(f'./{filename}', 'r') as f:
            data = json.loads(f.read())

        # Break apart session results from the rest of the data
        # for easier loading into DuckDB
        session_results = []
        results = []
        for item in data:
            # Remove all keys except 'session_results' and 'subsession_id'
            session_results_obj = dict(item)
            keys_to_pop = []
            for key, _ in session_results_obj.items():
                if key != 'session_results' and key != 'subsession_id':
                    keys_to_pop.append(key)
                    
            for key in keys_to_pop:
                session_results_obj.pop(key)
                
            session_results.append(session_results_obj)
            
            results_obj = dict(item)
            results_obj.pop('session_results')
            results.append(results_obj)
        
        # Save the data to a new file
        session_results_filename = f'./{filename.split(".")[0]}_session_results.json'
        with open(session_results_filename, 'w') as f:
            f.write(json.dumps(session_results))
            
        results_filename = f'./{filename.split(".")[0]}_results.json'
        with open(results_filename, 'w') as f:
            f.write(json.dumps(results))
            
        # Upload the new files to B2
        bucket.upload_local_file(local_file=session_results_filename, file_name=f'STG/iRacing/results/session_results/{filename}')
        bucket.upload_local_file(local_file=results_filename, file_name=f'STG/iRacing/results/results/{filename}')
        
        s3 = BotoConnection.get_instance()
        buckett = s3.Bucket(bucket_name)
        print('/'.join(filepath))
        json_files = [obj.key for obj in buckett.objects.filter(Prefix='/'.join(filepath)) if obj.key == file]
        print([obj.key for obj in buckett.objects.filter(Prefix='/'.join(filepath))])
        if json_files:
            delete_response = buckett.delete_objects(
                Delete={
                    'Objects': [{'Key': key} for key in json_files]
                }
            )
            print(f'deleted the following json files: {json_files}')
        else:
            print('no json files found in the specified directory')
            print(f"Processing file: {file}")
        
    @task
    def convert_sessionresults_to_parquet():
        import duckdb
        from airflow.models import Variable
        import boto3

        db_filename = f'{uuid.uuid4()}.db'
        db = duckdb.connect(db_filename)
        
        connection_id = "backblaze_b2_default"
        connection = BaseHook.get_connection(connection_id)
            
        if not connection:
            print(f"No Airflow connection found with Conn Id '{connection_id}'.")
            raise ValueError(f"Airflow connection '{connection_id}' not found.")
        
        # Download the file
        s3_client = boto3.client('s3', aws_access_key_id=connection.login, aws_secret_access_key=connection.password, endpoint_url=connection.host)
        bucket_name = Variable.get('B2_BUCKET')
        s3_path = 'STG/iRacing/results/session_results/'

        # List all files in the specified S3 path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_path)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
        os.makedirs('temp', exist_ok=True)
        for file in files:
            filename = file.split('/')[-1]
            filepath = file.split('/')[:-1]
            s3_client.download_file(bucket_name, file, f'temp/{filename}')
        
        # Read the SQL query
        py_file_path = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(py_file_path, 'DDB_STG_SESSION_RESULTS.sql'), 'r') as f:
            query = f.read()
            
        # Execute the query to create the stg table
        try:
            db.sql(query)
        except Exception as e:
            if 'IO Error: No files found that match the pattern' in e.args[0]:
                print('No files found to process')
                return
            else:
                raise e
        
        # Define the output and upload filename and path
        output_filename = f'session_results_{uuid.uuid4()}.parquet'
        upload_filepath = f'STG/iRacing/results/session_results/{output_filename}'
        
        # Copy the flattened data to a parquet file. We will load into Clickhouse later.
        db.sql(f"COPY STG_SESSION_RESULTS TO '{output_filename}'")
        s3_client.upload_file(output_filename, bucket_name, upload_filepath)
        
        # Delete all JSON files that we just converted to a parquet
        filepath = s3_path.split('/')
        prefix = '/'.join(filepath)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            if json_files:
                delete_response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in json_files]
                    }
                )
                print(f'deleted the following json files: {json_files}')
            else:
                print('no json files found in the specified directory')
        else:
            print('no files found in the specified directory')
        
        # Clean up
        db.close()
        os.remove(db_filename)
        os.remove(output_filename)
        for file in os.listdir('temp'):
            if file.endswith('.json'):
                os.remove(f'temp/{file}')
        os.rmdir('temp')
        

        
        
        
        
    @task
    def load_sessionresults_to_clickhouse():
        import clickhouse_connect
        py_file_path = os.path.dirname(os.path.abspath(__file__))
        
        # Initialize the Clickhouse client
        db_conn = BaseHook.get_connection("clickhouse_prod")
        client = clickhouse_connect.get_client(host=db_conn.host, username=db_conn.login, password=db_conn.password, port=db_conn.port)
        
        # Get S3 credentials for Clickhouse
        s3_conn = BaseHook.get_connection("backblaze_b2_default")
        
        # Create the STG landing table
        with open(os.path.join(py_file_path, 'CH_stg_session_results.sql'), 'r') as f:
            query = f.read()
        client.command(query)
        
        # Insert the parquet data from S3 into the STG table.
        with open(os.path.join(py_file_path, 'CH_insert_stg_session_results.sql'), 'r') as f:
            query = f.read().format(
                access_key=s3_conn.login,
                secret_key=s3_conn.password,
                bucket=s3_conn.schema,
                endpoint=s3_conn.host
            )
        client.command(query)
        
        # Finally, insert the data into the final table
        with open(os.path.join(py_file_path, 'CH_insert_session_results.sql'), 'r') as f:
            query = f.read()
        client.command(query)
        
        import boto3
        s3_path = 'STG/iRacing/results/session_results/'
        bucket_name = Variable.get('B2_BUCKET')
        s3_client = boto3.client('s3', aws_access_key_id=s3_conn.login, aws_secret_access_key=s3_conn.password, endpoint_url=s3_conn.host)
        filepath = s3_path.split('/')
        prefix = '/'.join(filepath)
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            if json_files:
                delete_response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        'Objects': [{'Key': key} for key in json_files]
                    }
                )
                print(f'deleted the following json files: {json_files}')
            else:
                print('no json files found in the specified directory')
        else:
            print('no files found in the specified directory')

    @task
    def write_session_results():
        import clickhouse_connect
        py_file_path = os.path.dirname(os.path.abspath(__file__))
        
        # Initialize the Clickhouse client
        db_conn = BaseHook.get_connection("clickhouse_prod")
        client = clickhouse_connect.get_client(host=db_conn.host, username=db_conn.login, password=db_conn.password, port=db_conn.port)

        # truncate table and write new data 
        with open(os.path.join(py_file_path, 'CH_mv_session_results.sql'), 'r') as f:
            script = f.read()
        for query in script.split(';'):
            print(f'Query: {query}')
            if len(query) > 0:
                client.command(query)
                
    @task
    def write_dim_drivers():
        import clickhouse_connect
        py_file_path = os.path.dirname(os.path.abspath(__file__))
        
        # Initialize the Clickhouse client
        db_conn = BaseHook.get_connection("clickhouse_prod")
        client = clickhouse_connect.get_client(host=db_conn.host, username=db_conn.login, password=db_conn.password, port=db_conn.port)
        
        # truncate table and write new data
        with open(os.path.join(py_file_path, 'CH_dim_drivers.sql'), 'r') as f:
            script = f.read()
            
        for query in script.split(';'):
            print(f'Query: {query}')
            if len(query) > 0:
                client.command(query)

    
    # First, create a branching structure
    raw_files = find_raw_files()
    
    # Add a placeholder task that will run if there are no raw files
    empty_process = EmptyOperator(
        task_id='skip_pre_process',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # Modify pre_process to handle empty list case
    if raw_files:
        pre_process = pre_process_file_task.expand(file=raw_files)
        pre_process >> empty_process
    else:
        empty_process

    # Session results processing remains independent
    session_results = convert_sessionresults_to_parquet()
    load_sessionresults_stg = load_sessionresults_to_clickhouse()

    # Connect the branches using the empty operator
    empty_process >> session_results >> load_sessionresults_stg

    # Tasks for creating reporting tables
    write_session_results_to_tbl = write_session_results()
    write_dim_drivers_tbl = write_dim_drivers()

    load_sessionresults_stg >> write_session_results_to_tbl
    load_sessionresults_stg >> write_dim_drivers_tbl