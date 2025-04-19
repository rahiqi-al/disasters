"""
docker exec -it -u airflow airflow bash
pip show kaggle
pip install kaggle
docker-compose restart airflow
"""

MongoDB: """docker exec -it mongodb mongosh 
            use disaster 
            db.test_collection.insertOne({"name": "Test", "value": 123}) 
            db.test_collection.find()"""

Cassandra: """docker exec -it cassandra cqlsh
            CREATE KEYSPACE IF NOT EXISTS disaster WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            USE disaster;
            CREATE TABLE test_table (id int PRIMARY KEY, name text);

            INSERT INTO test_table (id, name) VALUES (1, 'Test');
            SELECT * FROM test_table;"""

kafka :"""create topic:
        docker exec kafka kafka-topics --create --topic test-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
        producer:
        docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server kafka:9092
        consumer:
        docker exec -it kafka kafka-console-consumer --topic test-topic --bootstrap-server kafka:9092 --from-beginning"""
        
nessie : http://localhost:19120/api/v1/trees , http://localhost:19120/tree/main
spark : http://localhost:8081/
airflow : http://localhost:8080/home
dremio : http://loca01lhost:9047/
minio : http://localhost:9001/


# since we used shared volumes no need to install in all of them just one is enough cause it will be mounted to all of them
"""
docker exec -it --user root spark-master bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
docker exec -it --user root spark-worker-1 bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
docker exec -it --user root spark-worker-2 bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
"""


docker-compose restart spark-master spark-worker-1 spark-worker-2


"""
The issue in earthquake_cleaning.py happens because you previously saved df_earthquake as an Iceberg table with Nessie, which created a metadata reference, 
but the metadata file is missing in Minio (e.g., deleted or not written). When you overwrite it, Nessie tries to read the old metadata first, fails, and throws a NotFoundException.
the same probleme would happend to droping the table cause neesie whould needs the metadata 

Best Solution:
Add this line to earthquake_cleaning.py before saveAsTable

spark.sql("DROP TABLE IF EXISTS nessie.silver.df_earthquake")"

or

to slove this just do this curl -X DELETE "<nessie-url>/trees/main/contents/silver.df_landslide"

or 

also use PURGE ensures the table and its metadata are fully removed from Nessie’s catalog, bypassing the missing file issue

or 

If you didn’t manually remove the table, .mode("overwrite") in saveAsTable should overwrite it
"""

"""
the dependecies probleme might be:
A transient network disruption, such as connectivity instability or server latency, prevented Spark from retrieving a critical 41MB dependency(like a shaky internet connection or server glitch)
"""


spark_submit:
-spark-submit --packages com.amazon.deequ:deequ:2.0.7-spark-3.5 installs the Deequ JAR in the cluster.
-In the session, .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5") specifies it’s used there.



---------------------------------------------------------
python -m mlflow ui --backend-store-uri file:/mlflow/mlruns --host 0.0.0.0 --port 8501


the MLflow server is the Tracking Server that hosts the backend  and is accessible via the MLflow UI
---------------------------------------------------------


- **Status**: Using `docker-compose`, MLflow UI (`http://localhost:5000`) fails because `airflow` container lacks port 5000 mapping.
- **Next Steps**:
  - **Edit `docker-compose.yml`**:
    - Open: `~/Desktop/disasters/docker-compose.yml`
    - Find `airflow` service (likely named `airflow`).
    - Add port mapping under `ports`:
      ```yaml
      services:
        airflow:
          ...
          ports:
            - "8080:8080"
            - "5000:5000"
          ...
      ```
  - **Apply Changes**:
    - Run: `docker-compose down`
    - Run: `docker-compose up -d`
  - **Start MLflow UI**:
    - Run: `docker exec -it -u airflow airflow bash`
    - Run: `python -m mlflow ui --backend-store-uri file:/mlflow/mlruns --host 0.0.0.0 --port 5000`
  - **Access UI**:
    - Open: `http://localhost:5000`
    - Verify: “Experiments” > `fire_weather_index_prediction` > runs (e.g., `897788a4ef7347fdb89d8b86a97d1e48`), “Models” > `LinearRegressionModel` (version 15).
  - **Check**:
    - Run: `docker ps`
    - Confirm: `0.0.0.0:5000->5000/tcp` for `airflow`.
- **Notes**:
  - If UI fails, try: `http://<host-ip>:5000` or check: `sudo lsof -i :5000`.
  - Ensure volume `disasters_airflow_mlflow` is unchanged in `docker-compose.yml`.



-Connecting Superset to Dremio:
 Start the Containers:

 docker-compose up -d.
 docker exec -it superset pip install sqlalchemy-dremio

-Initialize Superset:
 Execute these commands in the superset container:

 docker exec -it superset superset fab create-admin

 ""Username: admin
  First name: admin
  Last name: admin
  Email: ali123rahiqi@gmail.com
  Password: simplepassword123 (confirm same password twice).""

  docker exec -it superset superset db upgrade
  docker exec -it superset superset init
-Access Superset:
  Open http://localhost:8088.
  Log in with the admin credentials you set during create-admin (default: admin / admin if not changed).

-correct url:
 dremio+flight://ali:alialiali1@192.168.32.13:32010/dremio?UseEncryption=false


-Install Airflow Dependencies:
 docker exec -it -u airflow airflow bash
 pip install mlflow==2.17.2 psycopg2-binary boto3==1.33.13 minio
 pip show mlflow (2.17.2), pip show boto3 (1.33.13).
 exit



python -m mlflow ui --backend-store-uri file:/mlflow/mlruns --host 0.0.0.0 --port 8501



/mlflow/mlruns
├── <experiment_id_1> (e.g., 326335247399837062, fire_weather_index_prediction)
│   ├── meta.yaml
│   ├── <run_id_1> (e.g., 669b4312b20a479195e2b7c89ea169fd)
│   │   ├── artifacts
│   │   │   ├── model
│   │   │   │   ├── MLmodel
│   │   │   │   ├── model.pkl
│   │   │   │   ├── conda.yaml
│   │   │   │   ├── python_env.yaml
│   │   │   │   ├── requirements.txt
│   │   │   ├── scaler
│   │   │   │   ├── scaler.pkl
│   │   ├── metrics
│   │   │   ├── mae
│   │   │   ├── mse
│   │   │   ├── r2_score
│   │   ├── params
│   │   │   ├── test_size
│   │   │   ├── random_state
│   │   ├── tags
│   │   │   ├── mlflow.runName
│   │   │   ├── mlflow.source.type
│   │   ├── meta.yaml
├── <experiment_id_2> (e.g., 987654321098765432, cnn_fire_prediction)
│   ├── meta.yaml
│   ├── <run_id_2> (e.g., d6b604a2f55b4498b2edfdcdbc476965)
│   │   ├── artifacts
│   │   │   ├── model
│   │   │   │   ├── MLmodel
│   │   │   │   ├── data
│   │   │   │   │   ├── model
│   │   │   │   │   │   ├── assets
│   │   │   │   │   │   ├── fingerprint.pb
│   │   │   │   │   │   ├── keras_metadata.pb
│   │   │   │   │   │   ├── saved_model.pb
│   │   │   │   │   │   ├── variables
│   │   │   │   ├── conda.yaml
│   │   │   │   ├── python_env.yaml
│   │   │   │   ├── requirements.txt
│   │   ├── metrics
│   │   │   ├── accuracy
│   │   │   ├── precision
│   │   │   ├── recall
│   │   ├── params
│   │   │   ├── train_size
│   │   │   ├── val_size
│   │   │   ├── epochs
│   │   ├── tags
│   │   │   ├── mlflow.runName
│   │   │   ├── mlflow.source.type
│   │   ├── meta.yaml
├── .models
│   ├── LinearRegressionModel
│   │   ├── meta.yaml
│   │   ├── version-1
│   │   │   ├── meta.yaml
│   ├── CNNModel
│   │   ├── meta.yaml
│   │   ├── version-1
│   │   │   ├── meta.yaml


How to tell models apart in MinIO mlflow bucket:
Run ID: Unique folder (e.g., mlflow/669b4312b20a479195e2b7c89ea169fd/model.pkl for LinearRegression, mlflow/<new_run_id>/model.pth for CNN).
File Name: model.pkl (LinearRegression) vs. model.pth (CNN).


-Model Signature:
  Specifies input/output format.
  Prevents inference errors.
  Simplifies deployment.
  Improves model documentation.


-cnn_model:
  in airflow do this (pip install tensorflow  opencv-python matplotlib)(pip uninstall tensorflow-gpu)
  Keras limitation: tf.keras.utils.image_dataset_from_directory requires local folder paths
  MinIO uses object storage; paths like lake/bronze/wildfire-detection-image-data/forest_fire/ organize files, but aren't directories.



docker exec airflow ls /mlflow



both models saved in /mlflow/mlruns and MinIO can be loaded from MLflow to make predictions. The CNN model predicts fire/no-fire from images,
and the LinearRegression model predicts Fire Weather Index from features. All necessary artifacts (model weights, scaler for LinearRegression) are saved for this purpose.


if not explicitly set, the artifact URI follows the tracking URI (file:/mlflow/mlruns)





-Solution to jars :
 ==>Create Local Folder Outside Project:
      mkdir -p ~/.spark-jars/cache ~/.spark-jars/jars
      sudo chown -R 1001:1001 ~/.spark-jars
 ==>Download JARs to Local Folder Run once to download JARs:
      docker run --rm -v $HOME/.spark-jars:/spark-jars bitnami/spark:3.5.1 bash -c "spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,com.amazon.deequ:deequ:2.0.7-spark-3.5 --conf spark.jars.ivy=/spark-jars /tmp/dummy.py && mv /spark-jars/*.jar /spark-jars/jars/ 2>/dev/null || true"
      docker run --rm -v $HOME/.spark-jars:/spark-jars bitnami/spark:3.5.1 bash -c "spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.jars.ivy=/spark-jars /tmp/dummy.py && mv /spark-jars/*.jar /spark-jars/jars/ 2>/dev/null || true"
 ==>Update docker-compose.yml Mount ~/.spark-jars to /opt/bitnami/spark/.ivy2
 ==>Verify:
      ls -R ~/.spark-jars/jars

 ""(Spark will automatically use the JARs in ~/.spark-jars (mounted to /opt/bitnami/spark/.ivy2) without downloading. The --packages command checks the cache first and finds the pre-downloaded JARs. No re-downloads occur after the initial setup)""


-response.raise_for_status() raises an HTTPError if the HTTP request returned an unsuccessful status code (like 4xx or 5xx). If the request was successful (2xx), it does nothing.


-rain data (rain.1h) is not always missing in OpenWeatherMap API responses. It’s only absent when no precipitation occurred in the last hour. For Chefchaouen, where rainfall is often low, Rain: 0.0 is a valid default and usually accurate
 i tested this using postman (https://api.openweathermap.org/data/2.5/weather?q=casablanca,MA&appid=040fc17d4596ec7b20a406eb4a96eb8d&units=metric)





for producer we need to install confluent_kafka (pip install confluent_kafka )





Créer_topic
docker exec -it kafka kafka-topics --create --topic weather_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Publier_message_topic
docker exec -it kafka kafka-console-producer --broker-list localhost:9092 --topic weather_data

Consommer_messages_topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_data --from-beginning


also to run the consumer inside the spark container i need to install this (pip install  pymongo  cassandra-driver)




docker exec -it mongodb mongosh
use weatherDB
db.weather_data.find().pretty()
db.weather_data.drop()


docker exec -it cassandra cqlsh
USE weather_keyspace;
SELECT * FROM weather_data;



we need to innstall inside airflow container (pip install apache-airflow-providers-amazon boto3) so we could use S3keysensor



-Set Up MinIO Connection in Airflow:
  Go to Airflow UI → Admin → Connections → Add:
  Conn Id: minio_conn
  Conn Type: Amazon Web Services
  Extra:
        {
          "aws_access_key_id": "ali",
          "aws_secret_access_key": "aliali123",
          "endpoint_url": "http://minio:9000"
        }