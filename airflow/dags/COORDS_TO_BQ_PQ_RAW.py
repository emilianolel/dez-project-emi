from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

SRC_SHELL_PATH = '/opt/airflow/src/coords_to_bq_pq_raw/shell/'

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'retries': 0,
}


with DAG(
    'COORDS_TO_BQ_PQ_RAW',
    default_args=default_args,
    schedule_interval=None,
    catchup = False,
    template_searchpath=SRC_SHELL_PATH
) as dag:

    start = DummyOperator(task_id='start')

    start_cluster = BashOperator(
        task_id='start_cluster',
        bash_command='start_cluster.sh'
    )

    download_coords = BashOperator(
        task_id='download_coords',
        bash_command='01_download_mex_coords_csv.sh'
    )

    csv_to_pq = BashOperator(
        task_id='csv_to_pq',
        bash_command='02_csv_to_pq.sh'
    )

    from_gcs_to_bq= BashOperator(
        task_id='from_gcs_to_bq',
        bash_command=f'03_gsc_pq_to_bq_table.sh'
    )

    stop_cluster = BashOperator(
        task_id='stop_cluster',
        bash_command='stop_cluster.sh'
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies

    start >> [start_cluster, download_coords] >> csv_to_pq >> from_gcs_to_bq >> stop_cluster >> end

    #start >> csv_to_pq >> end
