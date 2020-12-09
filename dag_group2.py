from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_dag_args = {
    'start_date': datetime.now()
}
dag = DAG(
    dag_id='your_dag_name',
    schedule_interval = timedelta(days=1),
    default_args=default_dag_args)

task1 = BashOperator(
task_id = "task1",
bash_command = "echo hello world 1",
dag = dag)  

task2 = BashOperator(
task_id = "task2",
bash_command = "echo hello world 2",
dag = dag) 

task3 = BashOperator(
task_id = "task3",
bash_command = "echo hello world 3",
dag = dag)

task1 >> task2 >> task3