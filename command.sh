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