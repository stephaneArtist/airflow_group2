from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

dag = DAG(
dag_id = "group2",
start_date = "2020-12-09",
schedule_interval='@daily')

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