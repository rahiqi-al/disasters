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