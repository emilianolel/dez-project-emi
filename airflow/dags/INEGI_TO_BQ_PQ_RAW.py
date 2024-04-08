from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

SRC_SHELL_PATH = '/opt/airflow/src/inegi_to_bq_pq_raw/shell/'

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'retries': 0,
}


with DAG(
    'INEGI_TO_BQ_PQ_RAW',
    default_args=default_args,
    schedule_interval=None,
    catchup = False,
    template_searchpath=SRC_SHELL_PATH,
    max_active_runs = 1
) as dag:

    start = DummyOperator(task_id='start')

    start_cluster = BashOperator(
        task_id='start_cluster',
        bash_command='start_cluster.sh'
    )

    download_inegi = BashOperator(
        task_id='download_inegi',
        bash_command='01_data_inegi.sh'
    )

    stop_cluster = BashOperator(
        task_id='stop_cluster',
        bash_command='stop_cluster.sh'
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies

    start >> [start_cluster, download_inegi] >> stop_cluster >> end

    #start >> csv_to_pq >> end
