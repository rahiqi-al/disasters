import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,col,when,split,regexp_replace , trim

from batch.batchConfig.config import config
import logging

logger=logging.getLogger(__name__)


def fill_coordinates(df):
    coord_map = {
        ("USA", "COOK INLET, AK"): (-151.0, 60.0),
        ("TAIWAN", "TAIWAN"): (121.0, 23.0),
        ("USA", "HAWAII"): (-157.0, 20.0),
        ("USA", "WASHINGTON-OREGON"): (-123.0, 46.0),
        ("COOK ISLANDS", "COOK IS."): (-159.0, -21.0),
        ("VANUATU", "MALO PASS"): (167.0, -15.5),
        ("TOGO", "GOLD COAST"): (1.0, 6.0),
        ("MARSHALL ISLANDS, REP. OF", "MARSHALL ISLANDS"): (171.0, 7.0),
        ("USA", "LOUISIANA: GRAND ISLE"): (-90.0, 29.0),
        ("NEW ZEALAND", "LAKE TAUPO, NORTH ISLAND"): (175.9, -38.7),
        ("USA", "SAN FRANCISCO, CA"): (-122.4, 37.8),
        ("MICRONESIA, FED. STATES OF", "YAP ISLANDS, CAROLINE ISLANDS"): (138.1, 9.5),
        ("USA", "SANTA BARBARA, S. CALIFORNIA"): (-119.7, 34.4),
        ("INDONESIA", "INDONESIA"): (120.0, -5.0),
        ("PAPUA NEW GUINEA", "HUON GULF, SOLOMON SEA"): (147.0, -6.0),
        ("COSTA RICA", "COSTA RICA-PANAMA"): (-83.0, 9.0),
        ("DOMINICAN REPUBLIC", "SANTO DOMINGO"): (-69.9, 18.5),
        ("FRENCH POLYNESIA", "MAUPIHAA"): (-152.0, -16.5),
        ("USA", "GULF OF MEXICO"): (-90.0, 25.0),
        ("AUSTRALIA", "TASMAN SEA"): (150.0, -40.0),
        ("CHILE", "NORTHERN CHILE"): (-70.0, -20.0),
        ("KOREA", "KOREA"): (127.0, 37.0),
        ("USA", "N. CALIFORNIA"): (-122.0, 40.0),
        ("RUSSIA", "TATAR STRAIT"): (141.0, 47.0),
        ("PHILIPPINES", "W. LUZON ISLAND"): (120.0, 16.0),
        ("ITALY", "CALABRIAN ARC"): (16.0, 38.0),
        ("USA", "CALIFORNIA"): (-120.0, 36.0),
        ("PHILIPPINES", "SULU SEA"): (121.0, 6.0),
        ("USA", "SE. ALASKA, AK"): (-135.0, 58.0),
        ("GREECE", "AEGEAN SEA"): (25.0, 38.0),
        ("VENEZUELA", "CARUPANO"): (-63.0, 10.6),
        ("USA", "GRAND HAVEN, MICHIGAN"): (-86.2, 43.0),
        ("MEXICO", "S. MEXICO"): (-97.0, 16.0),
        ("USA", "WILLETTS POINT, NEW YORK"): (-73.8, 40.8),
        ("PORTUGAL", "AZORES"): (-28.0, 38.5),
        ("NEW ZEALAND", "GOOSE BAY, SOUTH ISLAND"): (173.0, -42.0),
        ("AUSTRALIA", "BONDI BEACH, SYDNEY"): (151.3, -33.9),
        ("USA", "HOLLAND, MICHIGAN"): (-86.1, 42.8),
        ("USA", "COLUMBIA RIVER VALLEY, WA"): (-122.0, 46.0),
        ("CHINA", "CHINA: GANSU AND SHANXI PROVINCES"): (105.0, 36.0),
        ("PERU", "PERU"): (-76.0, -10.0),
        ("CHILE", "SOUTHERN CHILE"): (-73.0, -40.0),
        ("ITALY", "AEGEAN SEA"): (25.0, 38.0),
        ("USA", "GULF OF ALASKA, AK"): (-140.0, 58.0),
        ("USA TERRITORY", "NORTHERN COAST OF PUERTO RICO"): (-66.5, 18.5),
        ("SOUTH AFRICA", "SOUTH AFRICA"): (25.0, -30.0),
        ("INDIA", "INDIA"): (78.0, 20.0),
        ("ITALY", "EASTERN ITALY, ADRIATIC SEA"): (13.0, 43.0),
        ("JAPAN", "NAGASAKI, JAPAN"): (129.9, 32.7),
        ("USA", "S. CALIFORNIA"): (-118.0, 34.0),
        ("USA", "DAYTONA BEACH, FL"): (-81.0, 29.2),
        ("AUSTRALIA", "NORFOLK ISLAND"): (167.9, -29.0),
        ("USA", "WARREN DUNES, MICHIGAN"): (-86.6, 41.9),
        ("CANADA", "BRITISH COLUMBIA"): (-125.0, 50.0),
        ("SOUTH KOREA", "YELLOW SEA"): (125.0, 35.0),
        ("SOUTH KOREA", "CHEJU (JEJU) STRAIT"): (126.5, 33.5),
        ("USA", "NORTHWEST ATLANTIC OCEAN"): (-70.0, 40.0),
        ("SPAIN", "MEDITERRANEAN AND BLACK SEAS"): (0.0, 38.0),
        ("SCOTLAND", "NORTH SEA"): (-2.0, 58.0),
        ("NETHERLANDS", "NORTH SEA"): (4.0, 54.0),
        ("USA", "NORTHEAST U.S.A."): (-73.0, 42.0),
        ("SPAIN", "BALEARIC ISLANDS"): (2.5, 39.5),
        ("USA", "LAKE ERIE"): (-81.0, 42.0),
        ("USA", "GULF OF MEXICO"): (-90.0, 25.0)
    }

    for (country, location), (lon, lat) in coord_map.items():
        df = df.withColumn("Longitude",
                        when((col("Country") == country) & (col("Location") == location) & (col("Longitude").isNull()), lon)
                        .otherwise(col("Longitude"))) \
            .withColumn("Latitude",
                        when((col("Country") == country) & (col("Location") == location) & (col("Latitude").isNull()), lat)
                        .otherwise(col("Latitude")))

    return df


try :
    conf = (
        pyspark.SparkConf()
        .setAppName("IcebergNessieMinio")
        .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", config.nessie_uri)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", config.warehouse)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")  
        .set("spark.hadoop.fs.s3a.access.key", config.aws_access_key)
        .set("spark.hadoop.fs.s3a.secret.key", config.aws_secret_key)
        .set("spark.hadoop.fs.s3a.endpoint", config.aws_s3_endpoint)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.sql.defaultCatalog", "nessie")
    )
    logger.info("Spark configuration initialized successfully")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger.info("Spark session created successfully")

    logger.info("Reading tsunami data from minio")
    df_tsunami=spark.read.csv(f"s3a://{config.bucket_name}/{config.bronze_prefix}tsunami-events-dataset-1900-present/tsunamis-2023-09-11_22-13-51_ 0530 (2).csv",header=True,inferSchema=True)

    df_tsunami = df_tsunami.select(*config.selected_columns_tsunami).toDF(*config.new_column_names_tsunami)

    logger.info('cleaning the tsunami dataset')

    df_tsunami = df_tsunami.withColumn("Country", regexp_replace("Country", "Nan", " ")).withColumn('Country',trim('Country'))
    
    df_tsunami = df_tsunami.withColumn("Location", regexp_replace("Location", "Nan", " ")).withColumn('Location',trim('Location'))


    df_tsunami = fill_coordinates(df_tsunami)

    df_tsunami=df_tsunami.filter(col('Tsunami Event Validity')!=-1)

    df_tsunami=df_tsunami.filter(col('Mo').isNotNull()|col('Dy').isNotNull())

    df_tsunami = df_tsunami.withColumn("Tsunami Event Validity",when(col("Tsunami Event Validity") == 0, "event that only caused a seiche")
                                       .when(col("Tsunami Event Validity") == 1, "very doubtful tsunami")
                                        .when(col("Tsunami Event Validity") == 2, "questionable tsunami")
                                        .when(col("Tsunami Event Validity") == 3, "probable tsunami")
                                        .when(col("Tsunami Event Validity") == 4, "definite tsunami")
                                        .otherwise(col("Tsunami Event Validity")))
    

    df_tsunami = df_tsunami.withColumn("Tsunami Cause Code",when(col("Tsunami Cause Code") == 0, "Unknown")
                                       .when(col("Tsunami Cause Code") == 1, "Earthquake")
                                        .when(col("Tsunami Cause Code") == 2, "Questionable Earthquake")
                                        .when(col("Tsunami Cause Code") == 3, "Earthquake and Landslide")
                                        .when(col("Tsunami Cause Code") == 4, "Volcano and Earthquake")
                                        .when(col("Tsunami Cause Code") == 5, "Volcano, Earthquake, and Landslide")
                                        .when(col("Tsunami Cause Code") == 6, "Volcano")
                                        .when(col("Tsunami Cause Code") == 7, "Volcano and Landslide")
                                        .when(col("Tsunami Cause Code") == 8, "Landslide")
                                        .when(col("Tsunami Cause Code") == 9, "Meteorological")
                                        .when(col("Tsunami Cause Code") == 10, "Explosion")
                                        .when(col("Tsunami Cause Code") == 11, "Astronomical Tide")
                                        .otherwise(col("Tsunami Cause Code")))
    

    df_tsunami = df_tsunami.withColumn("Death Description",when(col("Death Description") == 0, "None")
                                       .when(col("Death Description") == 1, "Few (~1 to 50 deaths)")
                                       .when(col("Death Description") == 2, "Some (~51 to 100 deaths)")
                                       .when(col("Death Description") == 3, "Many (~101 to 1000 deaths)")
                                       .when(col("Death Description") == 4, "Very many (over 1000 deaths)")
                                       .otherwise(col("Death Description")))
    

    df_tsunami = df_tsunami.withColumn("Injuries Description",when(col("Injuries Description") == 0, "None")
                                       .when(col("Injuries Description") == 1, "Few (~1 to 50 injuries)")
                                       .when(col("Injuries Description") == 2, "Some (~51 to 100 injuries)")
                                       .when(col("Injuries Description") == 3, "Many (~101 to 1000 injuries)")
                                       .when(col("Injuries Description") == 4, "Very many (over 1000 injuries)")
                                       .otherwise(col("Injuries Description")))
    

    logger.info('the tsunami dataset is cleaned')


    logger.info("Creating namespace nessie.silver if it doesn't exist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    logger.info("Writing tsunami data to Iceberg table nessie.silver.df_tsunami")
    spark.sql("DROP TABLE IF EXISTS nessie.silver.df_tsunami")
    df_tsunami.write.format("iceberg").mode("overwrite").saveAsTable("nessie.silver.df_tsunami")
    logger.info("tsunami data successfully written to Iceberg table")


except ValueError as ve:
    logger.error(f"Data processing failed due to invalid value: {ve}")
    raise
except IOError as ioe:
    logger.error(f"S3(minio) access or file operation failed: {ioe}")
    raise
except Exception as e:
    logger.exception(f"Unexpected error: {e}")
    raise

finally :
    logger.info("Spark session stopped")
    spark.stop()


    