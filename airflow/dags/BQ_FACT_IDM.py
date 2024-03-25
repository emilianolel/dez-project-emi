from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

SRC_SHELL_PATH = '/opt/airflow/src/bq_fact_idm/shell/'

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'retries': 0,
}


with DAG(
    'BQ_FACT_IDM',
    default_args=default_args,
    schedule_interval=None,
    catchup = False,
    template_searchpath=SRC_SHELL_PATH,
    max_active_runs = 1
) as dag:

    start = DummyOperator(task_id='start')

    run_models = BashOperator(
        task_id='run_models',
        bash_command='01_build_models.sh'
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies

    start >> run_models >> end
