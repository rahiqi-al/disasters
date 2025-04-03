from dotenv import load_dotenv
import os 
import yaml 
load_dotenv()



class Config :
    with open('/project/batch/batchConfig/config.yml','r') as file:
        config_data = yaml.load(file , Loader=yaml.FullLoader)

        datasets = config_data["INGESTION"]['DATASETS']
        bucket_name = config_data["INGESTION"]['BUCKET']
        bronze_prefix = config_data["INGESTION"]['BRONZE']




        access_key = os.getenv('ACCESS_KEY')
        secret_key = os.getenv('SECRET_KEY')






config = Config()    
#print(config.access)
