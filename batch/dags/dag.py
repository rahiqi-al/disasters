import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta

import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.ingestion import ingestion
from batchConfig.config import config

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',filename="logs/app.log",filemode='a')
logger=logging.getLogger(__name__)

args={ 
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'retries': 1,
    'execution_timeout': timedelta(hours=1),
    'retry_delay': timedelta(minutes=3)}


def check_success():
    print("success")

with DAG('disasters', default_args=args, start_date=datetime(2025,1,1), schedule_interval='@yearly', catchup=False ) as dag :
    ingestion_tasks = [PythonOperator(task_id=f"ingest--{dataset.replace('/', '-')}", python_callable=ingestion, op_kwargs={"dataset": dataset}) for dataset in config.datasets ]
    success = PythonOperator(task_id='success', python_callable=check_success)




    ingestion_tasks >> success



    