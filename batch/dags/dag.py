import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime , timedelta
from batch.scripts.ingestion import ingestion 
from batch.scripts.check_kaggle import check_kaggle_api
from batch.batchConfig.config import config
from batch.scripts.regression_model import train_and_track_model 
from batch.scripts.cnn_model import cnn_model 

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',filename="/opt/airflow/logs/app.log",filemode='a')
logger=logging.getLogger(__name__)

args={ 
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'retries': 1,
    'execution_timeout': timedelta(hours=1),
    'retry_delay': timedelta(minutes=3)}


def on_failure_callback(context):
    task_instance = context['task_instance']
    logger.error(f"Task {task_instance.task_id} failed. DAG: {task_instance.dag_id} at {datetime.now()}")

def check_success():
    print("success")

with DAG('disasters', default_args=args, start_date=datetime(2025,1,1), schedule_interval='@yearly', catchup=False ,on_failure_callback=on_failure_callback) as dag :
    check_kaggle_api = PythonSensor(task_id='check_kaggle_api', python_callable=check_kaggle_api, poke_interval=60, timeout=600)
    #ingestion_tasks = [PythonOperator(task_id=f"ingest--{dataset.replace('/', '-')}", python_callable=ingestion, op_kwargs={"dataset": dataset}) for dataset in config.datasets ]
    success = PythonOperator(task_id='success', python_callable=check_success)
    #spark_task = BashOperator(task_id='spark_test_task',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/test.py')
    eartquake_cleaning = BashOperator(task_id='eartquake_cleaning',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/earthquake_cleaning.py')
    volcano_cleaning = BashOperator(task_id='volcano_cleaning',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/volcano_cleaning.py')
    tsunami_cleaning = BashOperator(task_id='tsunami_cleaning',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/tsunami_cleaning.py')
    landslide_cleaning = BashOperator(task_id='landslide_cleaning',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/landslide_cleaning.py')
    galax_modelisation = BashOperator(task_id='galax_modelisation',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0  /project/batch/scripts/galaxy_schema_modeling.py')
    data_quality_check = BashOperator(task_id='data_quality_check',bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,com.amazon.deequ:deequ:2.0.7-spark-3.5  /project/batch/Data-quality/data_quality_check.py')
    model_regression = PythonOperator(task_id="model_regression",python_callable=train_and_track_model)
    cnn_fire_model = PythonOperator(task_id="cnn_model",python_callable=cnn_model)
    ingestion_tasks = {
        f"ingest--{dataset.replace('/', '-')}": PythonOperator(
            task_id=f"ingest--{dataset.replace('/', '-')}",
            python_callable=ingestion,
            op_kwargs={"dataset": dataset}
        ) for dataset in config.datasets
    }




    check_kaggle_api>>list(ingestion_tasks.values())
    ingestion_tasks["ingest--joebeachcapital-earthquakes"] >> eartquake_cleaning
    ingestion_tasks["ingest--harshalhonde-tsunami-events-dataset-1900-present"] >> tsunami_cleaning
    ingestion_tasks["ingest--mexwell-significant-volcanic-eruption-database"] >> volcano_cleaning
    ingestion_tasks["ingest--kazushiadachi-global-landslide-data"] >> landslide_cleaning
    ingestion_tasks["ingest--brsdincer-wildfire-detection-image-data"] >> cnn_fire_model
    ingestion_tasks["ingest--sudhanshu432-algerian-forest-fires-cleaned-dataset"] >> model_regression
    
    [eartquake_cleaning,tsunami_cleaning,volcano_cleaning,landslide_cleaning] >> galax_modelisation

    galax_modelisation>> data_quality_check

    [cnn_fire_model,model_regression] >> success

    #check notebook 




    