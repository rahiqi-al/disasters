from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, udf , lit
from pyspark.sql.types import StructType, StructField, FloatType
from pymongo import MongoClient
from stream.streamConfig.config import config  
from cassandra.cluster import Cluster, NoHostAvailable
import uuid
import json
import logging
import math
from datetime import datetime

logger = logging.getLogger(__name__)

def calculate_ffmc(temp, rh, ws, rain, prev_ffmc=85.0):
    if prev_ffmc is None:
        prev_ffmc = 85.0
    m0 = (147.2 * (101.0 - prev_ffmc)) / (59.5 + prev_ffmc)
    if rain > 0.5:
        rw = rain - 0.5
        m0 += 42.5 * rw * (1.0 - 0.0015 * rw)
    m = m0 - 250.0 * (1.0 - rh / 100.0)
    if m < 0:
        m = 0
    ffmc = (59.5 * (250.0 - m)) / (147.2 + m)
    return max(28.6, min(ffmc, 92.5))

def calculate_dmc(temp, rh, rain, prev_dmc=6.0, month=datetime.now().month):
    if prev_dmc is None:
        prev_dmc = 6.0
    drying_factor = [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5][month - 1]
    if rain > 1.5:
        re = 0.92 * rain - 1.27
        mo = 20.0 + 280.0 / (1.0 + 280.0 / prev_dmc)
        b = 100.0 / (0.5 * prev_dmc) if prev_dmc > 0 else 100.0
        mo = mo * (1.0 - 0.001 * re * b)
        prev_dmc = mo / (1.0 + mo / 280.0)
    if temp > -1.1:
        sl = 1.6 * (temp + 1.1) * (100.0 - rh) / 1000.0
        prev_dmc += drying_factor * sl
    return max(1.1, min(prev_dmc, 65.9))

def calculate_dc(temp, rain, prev_dc=15.0, month=datetime.now().month):
    if prev_dc is None:
        prev_dc = 15.0
    drying_factor = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6][month - 1]
    if rain > 2.8:
        rd = 0.83 * rain - 1.27
        prev_dc += 400.0 * (rd / (3.937 + prev_dc))
    if temp > -2.8:
        prev_dc += 0.36 * (temp + 2.8) * drying_factor
    return max(7.0, min(prev_dc, 220.4))

def calculate_isi(ffmc, ws):
    f_wind = math.exp(0.05039 * ws)
    m = (147.2 * (101.0 - ffmc)) / (59.5 + ffmc)
    f_fuel = 19.115 * math.exp(-0.1386 * m) * (1.0 + m**5.31 / 4.93e7)
    isi = f_fuel * f_wind
    return max(0.0, min(isi, 18.5))

def calculate_bui(dmc, dc):
    if dmc <= 0.4 * dc:
        bui = dmc + (1.0 - 0.8 * dmc / (dmc + dc)) * (0.1 * dc)
    else:
        bui = dmc - (1.0 - 0.8 * dmc / (dmc + dc)) * (0.1 * dc)
    return max(1.1, min(bui, 68.0))

# register UDFs
ffmc_udf = udf(calculate_ffmc, FloatType())
dmc_udf = udf(calculate_dmc, FloatType())
dc_udf = udf(calculate_dc, FloatType())
isi_udf = udf(calculate_isi, FloatType())
bui_udf = udf(calculate_bui, FloatType())

def store_mongodb(batch_df, batch_id):
    client = None
    try:
        client = MongoClient(config.mongodb_connection)
        db = client["weatherDB"]
        collection = db["weather_data"]
        
        results = batch_df.toJSON().collect()
        logger.info(f"Batch {batch_id}: Collected {len(results)} JSON rows")
        data = [json.loads(r) for r in results]
        
        if data:
            collection.insert_many(data)
            logger.info(f"Inserted {len(data)} records into MongoDB for batch {batch_id}")
        else:
            logger.info(f"No data to insert into MongoDB for batch {batch_id}")
    
    except Exception as e:
        logger.exception(f"MongoDB error in batch {batch_id}: {e}")
    finally:
        if client:
            client.close()

def store_cassandra(batch_df, batch_id):
    cluster = None
    try:    
        logger.info(f'Storing batch {batch_id} in Cassandra')
        cluster = Cluster([config.cassandra_host], port=9042)
        session = cluster.connect()

        session.execute("CREATE KEYSPACE IF NOT EXISTS weather_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.set_keyspace("weather_keyspace")
        
        session.execute("""CREATE TABLE IF NOT EXISTS weather_keyspace.weather_data (
            id uuid PRIMARY KEY,
            temperature float,
            rh float,
            ws float,
            rain float,
            ffmc float,
            dmc float,
            dc float,
            isi float,
            bui float,
            ingestion_time timestamp)""")
        
        df = batch_df.select(
            col("Temperature").alias("temperature"),
            col("RH").alias("rh"),
            col("Ws").alias("ws"),
            col("Rain").alias("rain"))

        df = df.select(
            '*',
            ffmc_udf(col("temperature"), col("rh"), col("ws"), col("rain"), lit(85.0)).alias("ffmc"),
            dmc_udf(col("temperature"), col("rh"), col("rain"), lit(6.0), lit(datetime.now().month)).alias("dmc"),
            dc_udf(col("temperature"), col("rain"), lit(15.0), lit(datetime.now().month)).alias("dc")
        ).select(
            '*',
            isi_udf(col("ffmc"), col("ws")).alias("isi"),
            bui_udf(col("dmc"), col("dc")).alias("bui"),
            current_timestamp().alias("ingestion_time"))
        
        logger.info('Starting to insert data into Cassandra')
        rows = df.collect()
        for row in rows:
            session.execute(
                """INSERT INTO weather_keyspace.weather_data (
                    id, temperature, rh, ws, rain, ffmc, dmc, dc, isi, bui, ingestion_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    uuid.uuid4(), row["temperature"], row["rh"], row["ws"], row["rain"],
                    row["ffmc"], row["dmc"], row["dc"], row["isi"], row["bui"], row["ingestion_time"]
                )
            )
        logger.info(f"Finished inserting {len(rows)} rows into Cassandra for batch {batch_id}")
    
    except NoHostAvailable as e:
        logger.exception(f"Cassandra connection failed for batch {batch_id}: {e}")
    except Exception as e:
        logger.exception(f"Cassandra processing error for batch {batch_id}: {e}")
    finally:   
        if cluster: 
            cluster.shutdown()

def consumer():
    spark = None
    try:
        logger.info('Starting the consumer')
        spark = SparkSession.builder.appName("weather_streamprocessing").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1").getOrCreate()
        logger.info('Session created successfully')

        schema = StructType([StructField("Temperature", FloatType(), True),StructField("RH", FloatType(), True),StructField("Ws", FloatType(), True),StructField("Rain", FloatType(), True)])

        df = spark.readStream.format('kafka').option("kafka.bootstrap.servers", config.server).option("subscribe", config.topic_name).option("startingOffsets", "earliest").load()

        df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
        logger.info('Starting the function to store raw data in MongoDB and transformed to cassandra')
        query = df.writeStream.foreachBatch(lambda batch_df, batch_id: [store_mongodb(batch_df, batch_id), store_cassandra(batch_df, batch_id)]).outputMode('append').start()
        logger.info('Streaming query started, waiting for Kafka data...')

        query.awaitTermination()
    
    except Exception as e:
        logger.exception(f'Consumer error: {e}')
    finally:
        if spark:
            spark.stop()


consumer()