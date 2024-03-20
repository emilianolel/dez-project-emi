from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2022, 1, 1),
}

with DAG(
    dag_id="example1",
    default_args=default_args,
    catchup=False,
) as dag:
    t1 = BashOperator(task_id="t1", bash_command="sleep 5 && echo task 1")
    t2 = BashOperator(task_id="t2", bash_command="sleep 5 && echo task 2")
    t3 = BashOperator(task_id="t3", bash_command="sleep 15 && echo task 3")
    t4 = BashOperator(task_id="t4", bash_command="echo task 4")
    print("hola")
    t1 >> [t2, t3] >> t4
