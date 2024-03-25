from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'retries': 0,
}


with DAG(
    'IDM_MASTER_DAG',
    default_args=default_args,
    schedule_interval='0 0 22 * *',
    catchup = False,
    max_active_runs = 1
) as dag:

    start = DummyOperator(task_id='start')

    run_idm_to_bq_raw = TriggerDagRunOperator(
        task_id='run_idm_to_bq_raw',
        trigger_dag_id = 'IDM_TO_BQ_PQ_RAW',
        wait_for_completion = True
    )

    run_bq_fact_idm = TriggerDagRunOperator(
        task_id='run_bq_fact_idm',
        trigger_dag_id = 'BQ_FACT_IDM',
        wait_for_completion = True
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies

    start >> run_idm_to_bq_raw >> run_bq_fact_idm >> end
