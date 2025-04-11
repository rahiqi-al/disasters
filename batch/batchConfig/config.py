from dotenv import load_dotenv
import os 
import yaml 
load_dotenv('/project/.env')



class Config :
    with open('/project/batch/batchConfig/config.yml','r') as file:
        config_data = yaml.load(file , Loader=yaml.FullLoader)

        datasets = config_data["INGESTION"]['DATASETS']
        bucket_name = config_data["INGESTION"]['BUCKET']
        bronze_prefix = config_data["INGESTION"]['BRONZE']
        selected_columns_tsunami = config_data["CLEANING"]['SELECTED_COLUMNS_TS']
        new_column_names_tsunami = config_data["CLEANING"]['NEW_COLUMN_NAME_TS']
        selected_columns_ld = config_data["CLEANING"]['SELECTED_COLUMNS_LD']
        columns_to_fill_ld = config_data["CLEANING"]['COLUMNS_TO_FILL_LD']
        selected_columns_v = config_data["CLEANING"]['SELECTED_COLUMNS_V']





        access_key = os.getenv('ACCESS_KEY')
        secret_key = os.getenv('SECRET_KEY')
        aws_s3_endpoint = os.getenv('AWS_S3_ENDPOINT')
        nessie_uri = os.getenv('NESSIE_URI')
        aws_access_key = os.getenv('AWS_ACCESS_KEY')
        aws_secret_key = os.getenv('AWS_SECRET_KEY')
        warehouse = os.getenv('WAREHOUSE')






config = Config()    
#print(config.access)
