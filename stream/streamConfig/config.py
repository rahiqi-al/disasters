from dotenv import load_dotenv
import os 
import yaml 
load_dotenv()



class Config :
    with open('stream/streamConfig/config.yml','r') as file:
        config_data = yaml.load(file , Loader=yaml.FullLoader)










config = Config()    
#print(config.)