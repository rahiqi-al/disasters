stop & remove :  docker stop  ..... , docker rm .....
the_Local_PostgreSQL & Verify :  sudo systemctl stop postgresql  , sudo netstat -tulnp | grep 5432
logs : docker logs ....
Remove_old_volume_data : docker volume rm .....
kill_airflow_scheduler: pkill -9 -f "airflow scheduler"
Force_Kill_All_Airflow_Processes : pkill -9 -f "airflow"
--Airflow: http://localhost:8080
--Postgres_connection: postgresql+psycopg2://admin:admin@localhost:5432/airflow
--Dremio: http://localhost:9047
--MinIO:
    API: http://localhost:9000
    Console: http://localhost:9001
--Mongodb_Connection: mongodb://localhost:27017
--Cassandra_Connection: localhost:9042


docker-compose restart airflow



"""
MongoDB
docker exec -it mongodb mongosh

use disaster
db.test_collection.insertOne({"name": "Test", "value": 123})

db.test_collection.find()

Cassandra
docker exec -it cassandra cqlsh

CREATE KEYSPACE IF NOT EXISTS disaster WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE disaster;
CREATE TABLE test_table (id int PRIMARY KEY, name text);

INSERT INTO test_table (id, name) VALUES (1, 'Test');
SELECT * FROM test_table;

"""

"""
kafka 
create topic:
docker exec kafka kafka-topics --create --topic test-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
producer:
docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server kafka:9092
consumer:
docker exec -it kafka kafka-console-consumer --topic test-topic --bootstrap-server kafka:9092 --from-beginning
"""


"""

nessie
http://localhost:19120/api/v1/trees
http://localhost:19120/tree/main
spark
http://localhost:8081/
airflow
http://localhost:8080/home
dremio
http://localhost:9047/
"""

"""
docker exec -it -u airflow airflow bash
pip show kaggle
pip install kaggle
"""