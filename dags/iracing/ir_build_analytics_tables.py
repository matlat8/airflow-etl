from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator
)
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "start_date": "2024-12-31",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}
    
dag_id = "IrBuildAnalyticsTables"
dag_timeout = timedelta(hours=1)
with DAG(dag_id=dag_id, 
         default_args=default_args, 
         schedule_interval='@daily',
         tags=['iRacing', 'ETL', 'Data Modeling']
         ) as dag:
    
    @task.bash
    def git_pull():
        # return "ls"
        return """
                if [ -d "~/dbt/iracing" ]; then
                    cd ~/dbt/iracing && git pull
                else
                    mkdir -p ~/dbt && git clone https://github.com/matlat8/dbt_iracing ~/dbt/iracing
                fi
                ls ~/dbt/iracing
                """
                
    with TaskGroup(group_id='dim_drivers') as dim_drivers:
        create_dim_drivers = DbtRunOperator(
            task_id='dim_drivers',
            dir='/home/airflow/dbt/iracing',
            target='prod',
            models='dim_drivers',
            retries=0
        )

        test_dim_drivers = DbtTestOperator(
            task_id='test_dim_drivers',
            dir='/home/airflow/dbt/iracing',
            target='prod',
            models='dim_drivers',
            retries=0
        )
        
        create_dim_drivers >> test_dim_drivers
    
    gitpull = git_pull()
    gitpull  >> dim_drivers

    