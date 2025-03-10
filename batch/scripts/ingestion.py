import sys ,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from batchConfig.config import config
import kaggle
import minio


# first you need to create a .kaggle folder then move kaggle.jsoon to this folder 

#kaggle.api.authenticate()

def ingestion():
    pass    
