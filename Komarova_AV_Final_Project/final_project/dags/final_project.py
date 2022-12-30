from datetime import datetime, timedelta
import psycopg2

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from scripts import script_1_parsing

default_dag_args = {}

# Переменная с именем txt файла
file_path = Variable.get("final_file_path")
    
# ф-я для передачи креденшиалс    
def get_conn_credentials(connect_id) -> BaseHook.get_connection:
    conn_to_airflow = BaseHook.get_connection(connect_id)
    return conn_to_airflow

def hello():
    print("Hello!")


with DAG(dag_id=
    "final_project", start_date=datetime(2022, 12, 27),
    schedule_interval="0 12 29 12 *",
    default_args=default_dag_args,
) as dag:
    
    bash_hello = BashOperator(task_id="hello", bash_command="echo hello", do_xcom_push=False)
    parsing_init = BashOperator(task_id="parsing", \
            bash_command="/opt/airflow/dags/scripts/script_1_parsing.py", \
            do_xcom_push=False) 

 # Set dependencies between tasks
bash_hello >> parsing_init 