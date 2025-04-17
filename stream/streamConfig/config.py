from dotenv import load_dotenv
import os 
import yaml 
load_dotenv('/project/.env')



class Config :
    with open('/project/stream/streamConfig/config.yml','r') as file:
        config_data = yaml.load(file , Loader=yaml.FullLoader)


        weather_api_url = config_data['WEATHER_API_URL']
        topic_name = config_data['TOPIC_NAME']
        producer_config = config_data['PRODUCER_CONFIG']
        server = config_data['SERVER']
        mongodb_connection = config_data['MONGODB_CONNECTION']
        cassandra_host = config_data['CASSANDRA_HOST']


        api_key = os.getenv('API_KEY')


config = Config()    
#print(config.)