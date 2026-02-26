from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'my_first_dag',
    default_args=default_args,
    schedule_interval=None,
)

create_file = BashOperator(
    task_id='create_txt_file',
    bash_command='echo "test" > C:\\test.txt',
    dag=dag
)

create_file