import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta



import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.ingestion import ingestion

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',filename="logs/app.log",filemode='a')
logger=logging.getLogger(__name__)


args={ 
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'email': ['ali123rahiqi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'execution_timeout': timedelta(hours=1),
    'retry_delay': timedelta(minutes=3)}




with DAG('disasters', default_args=args, start_date=datetime(2025,1,1), schedule_interval='@yearly', catchup=False ) as dag :
    ingestion_to_bronze = PythonOperator(task_id='',python_callable=ingestion)




    ingestion_to_bronze