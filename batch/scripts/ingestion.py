from batch.batchConfig.config import config
from kaggle.api.kaggle_api_extended import KaggleApi
from minio import Minio
import tempfile
import logging
from minio.error import MinioException
import os,sys
logger=logging.getLogger(__name__)


# first you need to create a .kaggle folder then move kaggle.jsoon to this folder 
#you could use this instead 
#kaggle.api.authenticate()

def ingestion(dataset):

    try:
        
        logger.info(f'starting ingesting for {dataset}')

        api = KaggleApi()
        api.authenticate()


        logger.info(f'authentification successful for {dataset}')
        minio_client = Minio("minio:9000", access_key="rJPe0evFgnFb2SgK18xA", secret_key="T5YVbxm30sHqTOmxoApyVe0zLpS3ZklcivJNi6x3", secure=False)

        logger.info('checking the status of the bucket')

        if not minio_client.bucket_exists(config.bucket_name):
            minio_client.make_bucket(config.bucket_name)

        logger.info(f"Loading dataset: {dataset}")
        with tempfile.TemporaryDirectory() as temp_dir:

            api.dataset_download_files(dataset, path=temp_dir, unzip=True)

            for root, _, files in os.walk(temp_dir):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(file_path, temp_dir)
                    #so we dont take the user (so we dont make a folder under the username)
                    dataset_name = dataset.split("/")[-1]
                    object_name = f"{config.bronze_prefix}{dataset_name}/{relative_path}"
                    minio_client.fput_object(config.bucket_name, object_name, file_path)

                    logger.info(f"Uploaded to bronze: {object_name}") 
    
         
    except MinioException as me:
        logger.exception(f"MinIO error for dataset {dataset}: {str(me)}")
        raise # Re-raise to mark task as failed in Airflow(if you don’t add raise, the task will complete successfully (Airflow marks it as "success") even if an error occurs)
        
    except Exception as e:
        logger.exception(f"Unexpected error ingesting dataset {dataset}: {str(e)}")
        raise
