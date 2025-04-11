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
dremio : http://localhost:9047/
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
In landslide_cleaning.py, you didn’t save df_landslide as an Iceberg table before, so it starts fresh with no metadata conflict.
Adding .option("path", ...) or dropping the old table fixes df_earthquake by resetting its location or state. It’s a mismatch between Nessie’s catalog and Minio’s files specific to df_earthquake.
the same probleme would happend to droping the table cause neesie whould needs the metadata 

Best Solution:
Add this line to earthquake_cleaning.py before saveAsTableAdd this line to earthquake_cleaning.py before saveAsTable

spark.sql("DROP TABLE IF EXISTS nessie.silver.df_earthquake")"

or

to slove this just do this curl -X DELETE "<nessie-url>/trees/main/contents/silver.df_landslide"

or 

also use PURGE ensures the table and its metadata are fully removed from Nessie’s catalog, bypassing the missing file issue

or 

'If you didn’t manually remove the table, .mode("overwrite") in saveAsTable should overwrite it'
"""

"""
the dependecies probleme might be:
A transient network disruption, such as connectivity instability or server latency, prevented Spark from retrieving a critical 41MB dependency(like a shaky internet connection or server glitch)
"""


spark_submit:
-spark-submit --packages com.amazon.deequ:deequ:2.0.7-spark-3.5 installs the Deequ JAR in the cluster.
-In the session, .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.5") specifies it’s used there.
