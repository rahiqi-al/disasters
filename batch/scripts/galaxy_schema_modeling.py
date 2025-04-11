import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,col,when,split,regexp_replace , trim ,hash, abs, upper ,concat, lpad, monotonically_increasing_id

from batch.batchConfig.config import config
import logging

logger=logging.getLogger(__name__)



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

    logger.info("Reading all dataset  from minio to create the fact tables and dimensions")

    df_volcano = spark.read.format("iceberg").load("nessie.silver.df_volcano")
    df_landslide = spark.read.format("iceberg").load("nessie.silver.df_landslide")
    df_eartqueke = spark.read.format("iceberg").load("nessie.silver.df_earthquake")
    df_tsunami = spark.read.format("iceberg").load("nessie.silver.df_tsunami")

    logger.info('creating the dimensions')
    
    logger.info('creating dim_date')
    dim_date = spark.sql("""
        SELECT
            (EXTRACT(DAY FROM full_date) * 1000000 + EXTRACT(MONTH FROM full_date) * 10000 + EXTRACT(YEAR FROM full_date)) AS date_key,
            full_date,
            EXTRACT(YEAR FROM full_date) AS year,
            EXTRACT(MONTH FROM full_date) AS month,
            EXTRACT(DAY FROM full_date) AS day,
            EXTRACT(QUARTER FROM full_date) AS quarter,
            MOD(EXTRACT(DOW FROM full_date) + 6, 7) + 1 AS weekday,  -- 1=Monday, 7=Sunday
            EXTRACT(DOY FROM full_date) AS day_of_year
        FROM (
            SELECT explode(sequence(to_date('0037-01-01'), to_date('2023-12-31'), interval 1 day)) AS full_date
        )
    """)
    logger.info('dim_date created successfully')


    logger.info('creating dim_location')
    countries = (
        df_eartqueke.select(upper(col("Country")).alias("location")).distinct()
        .union(df_volcano.select(upper(col("Country")).alias("location")).distinct())
        .union(df_tsunami.select(upper(col("Country")).alias("location")).distinct())
        .union(df_landslide.select(upper(col("country_name")).alias("location")).distinct())).distinct()

    dim_location = countries.select(abs(hash(col("location"))).alias("location_key"),col("location"))
    logger.info('dim_location created successfully')

    logger.info('creating dim_volcano')
    dim_volcano = df_volcano.select(abs(hash(col("Volcano Name"))).alias("volcano_key"),col("Volcano Name").alias("volcano_name"),abs(col("Elevation")).alias("elevation"),col("Volcano Type").alias("volcano_type"),col("Status").alias("status")).filter(col("Volcano Name") != "Unnamed").dropDuplicates()
    logger.info('dim_volcano created successfully')

    logger.info('creating the dimensions of landslide')
    # 1. dim_landslide_category
    dim_landslide_category = df_landslide.select(abs(hash(col("landslide_category"))).alias("category_key"),col("landslide_category")).distinct()

    # 2. dim_landslide_trigger
    dim_landslide_trigger = df_landslide.select(abs(hash(col("landslide_trigger"))).alias("trigger_key"),col("landslide_trigger")).distinct()

    # 3. dim_landslide_size
    dim_landslide_size = df_landslide.select(abs(hash(col("landslide_size"))).alias("size_key"),col("landslide_size")).distinct()

    # 4. dim_landslide_setting
    dim_landslide_setting = df_landslide.select(abs(hash(col("landslide_setting"))).alias("setting_key"),col("landslide_setting")).distinct()
    logger.info('all dimensions of landslide created successfully')

    logger.info('creating the dimensions of tsunami')
    # 1. dim_tsunami_validity
    dim_tsunami_validity = df_tsunami.select(abs(hash(col("Tsunami Event Validity"))).alias("validity_key"),col("Tsunami Event Validity").alias("tsunami_event_validity")).distinct()

    # 2. dim_tsunami_cause
    dim_tsunami_cause = df_tsunami.select(abs(hash(col("Tsunami Cause Code"))).alias("cause_key"),col("Tsunami Cause Code").alias("tsunami_cause_code")).distinct()
    logger.info('all dimensions of tsunami created successfully')

    logger.info('9 dimensions (dim_date, dim_location, dim_volcano, dim_landslide_category, dim_landslide_trigger, dim_landslide_size, dim_landslide_setting, dim_tsunami_validity, dim_tsunami_cause) created successfully')

    logger.info('creating the fact tables')

    # 1.fact_volcano
    fact_volcano = df_volcano.filter(col("Volcano Name") != "Unnamed").select(
        (monotonically_increasing_id() + 1).alias("id_volcano"),
        abs(hash(col("Volcano Name"))).alias("volcano_key"),
        concat(lpad(col("Day").cast("string"), 2, "0"),lpad(col("Month").cast("string"), 2, "0"),lpad(col("Year").cast("string"), 4, "0")).alias("date_key"),
        abs(hash(upper(col("Country")))).alias("location_key"),
        col("Volcanic Explosivity Index"),
        col("Volcano : Deaths Description"),
        col("Total Effects : Deaths"),
        col("latitude"),
        col("longitude"))
    
    # 2.fact_earthquake
    fact_earthquake = df_eartqueke.select(
        col("ID Earthquake").alias("id_earthquake"),
        concat( lpad(col("Day").cast("string"), 2, "0"),  lpad(col("Month").cast("string"), 2, "0"),lpad(col("Year").cast("string"), 4, "0")).alias("date_key"),
        abs(hash(upper(col("Country")))).alias("location_key"),
        col("EQ Primary"),
        col("Earthquake : Deaths"),
        col("Earthquake : Deaths Description"),
        col("Earthquake : Damage Description"),
        col("Latitude"),
        col("Longitude"))
    
    # 3.fact_tsunami
    fact_tsunami = df_tsunami.select(
        col("id").alias("id_tsunami"),
        concat( lpad(col("Dy").cast("string"), 2, "0"),lpad(col("Mo").cast("string"), 2, "0"), lpad(col("Year").cast("string"), 4, "0")).alias("date_key"),
        abs(hash(upper(col("Country")))).alias("location_key"),
        abs(hash(col("Tsunami Event Validity"))).alias("validity_key"),
        abs(hash(col("Tsunami Cause Code"))).alias("cause_key"),
        col("Hr"),
        col("Mn"),
        col("Latitude"),
        col("Longitude"),
        col("Death Description"),
        col("Injuries Description"),
        col("Total Deaths"),
        col("Total Injuries"),
        col("Total Houses Destroyed"),
        col("Total Houses Damaged")).filter(col('date_key').isNotNull())
    
    # 4.fact_landslide
    fact_landslide = df_landslide.select(
        col("event_id").alias("id_landslide"),
        concat(lpad(col("Day").cast("string"), 2, "0"),lpad(col("Month").cast("string"), 2, "0"),lpad(col("Year").cast("string"), 4, "0")).alias("date_key"),
        abs(hash(upper(col("country_name")))).alias("location_key"),
        abs(hash(col("landslide_category"))).alias("category_key"),
        abs(hash(col("landslide_trigger"))).alias("trigger_key"),
        abs(hash(col("landslide_size"))).alias("size_key"),
        abs(hash(col("landslide_setting"))).alias("setting_key"),
        col("fatality_count"),
        col("injury_count"),
        col("longitude"),
        col("latitude"),
        col("Hour"),
        col("Minute"))
    
    logger.info('all facts are created successfully in total 4 facts')




    logger.info("Creating namespace nessie.gold if it doesn't exist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    
    logger.info("Writing  facts & dimensions to Iceberg table ")
    # save the dimensions
    dim_date.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_date")
    dim_location.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_location")
    dim_volcano.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_volcano")
    dim_tsunami_cause.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_tsunami_cause")
    dim_tsunami_validity.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_tsunami_validity")
    dim_landslide_trigger.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_landslide_trigger")
    dim_landslide_category.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_landslide_category")
    dim_landslide_setting.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_landslide_setting")
    dim_landslide_size.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.dim_landslide_size")
    # save the facts
    fact_earthquake.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.fact_earthquake")
    fact_landslide.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.fact_landslide")
    fact_tsunami.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.fact_tsunami")
    fact_volcano.write.format("iceberg").mode("overwrite").saveAsTable("nessie.gold.fact_volcano")
    # all facts and dimensions are saved 
    logger.info("facts & dimensions data successfully written to Iceberg table")


except ValueError as ve:
    logger.error(f"Data processing failed due to invalid value: {ve}")
    raise
except IOError as ioe:
    logger.error(f"S3 (Minio) access or file operation failed: {ioe}")
    raise
except Exception as e:
    logger.exception(f"Unexpected error: {e}")
    raise

finally :
    logger.info("Spark session stopped")
    spark.stop()


    