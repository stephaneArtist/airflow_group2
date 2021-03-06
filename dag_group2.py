from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

#from download-data import main

default_dag_args = {
    'owner': 'group2',
    'start_date': datetime.now(),
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    dag_id='group2_dag',
    schedule_interval = timedelta(minutes=1),
    default_args=default_dag_args
)

task1 = BashOperator(
    task_id = "task1",
    bash_command = "echo hello world",
    dag = dag
)

extract_data = BashOperator(
    task_id = "extract_data",
    bash_command = "python3 download-data.py",
    dag = dag
)    

spark_submit = BashOperator(
    task_id = "spark_submit",
    bash_command = "spark-submit --deploy-mode cluster --master yarn --class job.stat hdfs:///user/iabd2_group2/Stat.jar",
    dag = dag
)

""" extract_data = PythonOperator(
    task_id = "extract_data",
    python_callable = main,
    dag = dag
) 

task3 = BashOperator(
    task_id = "task3",
    bash_command = "echo hello world 3",
    dag = dag
)
task2 = BashOperator(
    task_id = "task2",
    bash_command = "echo hello world 2",
    dag = dag
)

 """
#task1 >> task2 >> task3
task1 >> extract_data >> spark_submit
