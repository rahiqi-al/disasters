import pyspark
from pyspark.sql import SparkSession
import os
os.environ["SPARK_VERSION"] = "3.5"  # Match your Spark version (e.g., 3.2, 3.3) 
import pydeequ
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import AnalysisRunner, Completeness, Uniqueness, Distinctness
from pyspark.sql.functions import col, when
from batch.batchConfig.config import config
import logging

logger=logging.getLogger(__name__)






try :


    conf = (
        pyspark.SparkConf()
        .setAppName("IcebergNessieMinio")
        .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazon.deequ:deequ:2.0.7-spark-3.5")
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
    

    logger.info('reading the facts and dimensions')
    # Read dimensions
    dim_date = spark.read.format("iceberg").load("nessie.gold.dim_date")
    dim_location = spark.read.format("iceberg").load("nessie.gold.dim_location")
    dim_volcano = spark.read.format("iceberg").load("nessie.gold.dim_volcano")
    dim_tsunami_cause = spark.read.format("iceberg").load("nessie.gold.dim_tsunami_cause")
    dim_tsunami_validity = spark.read.format("iceberg").load("nessie.gold.dim_tsunami_validity")
    dim_landslide_trigger = spark.read.format("iceberg").load("nessie.gold.dim_landslide_trigger")
    dim_landslide_category = spark.read.format("iceberg").load("nessie.gold.dim_landslide_category")
    dim_landslide_setting = spark.read.format("iceberg").load("nessie.gold.dim_landslide_setting")
    dim_landslide_size = spark.read.format("iceberg").load("nessie.gold.dim_landslide_size")

    # Read facts
    fact_earthquake = spark.read.format("iceberg").load("nessie.gold.fact_earthquake")
    fact_landslide = spark.read.format("iceberg").load("nessie.gold.fact_landslide")
    fact_tsunami = spark.read.format("iceberg").load("nessie.gold.fact_tsunami")
    fact_volcano = spark.read.format("iceberg").load("nessie.gold.fact_volcano")
    
    logger.info('starting the tests to check the data quality')
    # Verification Suite for running checks
    verificationSuite = VerificationSuite(spark)

    # Preprocess fact tables with join-based FK validation
    fact_earthquake_validated = fact_earthquake \
        .join(dim_date, fact_earthquake.date_key == dim_date.date_key, "left") \
        .withColumn("date_key_valid", when(dim_date.date_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_location, fact_earthquake.location_key == dim_location.location_key, "left") \
        .withColumn("location_key_valid", when(dim_location.location_key.isNotNull(), 1).otherwise(0)) \
        .drop(dim_date.date_key, dim_location.location_key)

    fact_volcano_validated = fact_volcano \
        .join(dim_date, fact_volcano.date_key == dim_date.date_key, "left") \
        .withColumn("date_key_valid", when(dim_date.date_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_location, fact_volcano.location_key == dim_location.location_key, "left") \
        .withColumn("location_key_valid", when(dim_location.location_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_volcano, fact_volcano.volcano_key == dim_volcano.volcano_key, "left") \
        .withColumn("volcano_key_valid", when(dim_volcano.volcano_key.isNotNull(), 1).otherwise(0)) \
        .drop(dim_date.date_key, dim_location.location_key, dim_volcano.volcano_key)

    fact_tsunami_validated = fact_tsunami \
        .join(dim_date, fact_tsunami.date_key == dim_date.date_key, "left") \
        .withColumn("date_key_valid", when(dim_date.date_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_location, fact_tsunami.location_key == dim_location.location_key, "left") \
        .withColumn("location_key_valid", when(dim_location.location_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_tsunami_validity, fact_tsunami.validity_key == dim_tsunami_validity.validity_key, "left") \
        .withColumn("validity_key_valid", when(dim_tsunami_validity.validity_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_tsunami_cause, fact_tsunami.cause_key == dim_tsunami_cause.cause_key, "left") \
        .withColumn("cause_key_valid", when(dim_tsunami_cause.cause_key.isNotNull(), 1).otherwise(0)) \
        .drop(dim_date.date_key, dim_location.location_key, dim_tsunami_validity.validity_key, dim_tsunami_cause.cause_key)

    fact_landslide_validated = fact_landslide \
        .join(dim_date, fact_landslide.date_key == dim_date.date_key, "left") \
        .withColumn("date_key_valid", when(dim_date.date_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_location, fact_landslide.location_key == dim_location.location_key, "left") \
        .withColumn("location_key_valid", when(dim_location.location_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_landslide_category, fact_landslide.category_key == dim_landslide_category.category_key, "left") \
        .withColumn("category_key_valid", when(dim_landslide_category.category_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_landslide_trigger, fact_landslide.trigger_key == dim_landslide_trigger.trigger_key, "left") \
        .withColumn("trigger_key_valid", when(dim_landslide_trigger.trigger_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_landslide_size, fact_landslide.size_key == dim_landslide_size.size_key, "left") \
        .withColumn("size_key_valid", when(dim_landslide_size.size_key.isNotNull(), 1).otherwise(0)) \
        .join(dim_landslide_setting, fact_landslide.setting_key == dim_landslide_setting.setting_key, "left") \
        .withColumn("setting_key_valid", when(dim_landslide_setting.setting_key.isNotNull(), 1).otherwise(0)) \
        .drop(dim_date.date_key, dim_location.location_key, dim_landslide_category.category_key,
            dim_landslide_trigger.trigger_key, dim_landslide_size.size_key, dim_landslide_setting.setting_key)

    # Dimension Checks
    dim_date_check = Check(spark, CheckLevel.Error, "dim_date") \
        .isComplete("date_key") \
        .isUnique("date_key") \
        .hasCompleteness("full_date", lambda x: x == 1.0)

    dim_location_check = Check(spark, CheckLevel.Error, "dim_location") \
        .isComplete("location_key") \
        .isUnique("location_key") \
        .isComplete("location")

    dim_volcano_check = Check(spark, CheckLevel.Error, "dim_volcano") \
        .isComplete("volcano_key") \
        .isUnique("volcano_key") \
        .isComplete("volcano_name") \
        .hasCompleteness("elevation", lambda x: x > 0.9) \
        .satisfies("elevation > 0", "Elevation Must Be Positive")

    dim_tsunami_validity_check = Check(spark, CheckLevel.Error, "dim_tsunami_validity") \
        .isComplete("validity_key") \
        .isUnique("validity_key") \
        .isComplete("tsunami_event_validity")

    dim_tsunami_cause_check = Check(spark, CheckLevel.Error, "dim_tsunami_cause") \
        .isComplete("cause_key") \
        .isUnique("cause_key") \
        .isComplete("tsunami_cause_code")

    dim_landslide_category_check = Check(spark, CheckLevel.Error, "dim_landslide_category") \
        .isComplete("category_key") \
        .isUnique("category_key") \
        .isComplete("landslide_category")

    dim_landslide_trigger_check = Check(spark, CheckLevel.Error, "dim_landslide_trigger") \
        .isComplete("trigger_key") \
        .isUnique("trigger_key") \
        .isComplete("landslide_trigger")

    dim_landslide_size_check = Check(spark, CheckLevel.Error, "dim_landslide_size") \
        .isComplete("size_key") \
        .isUnique("size_key") \
        .isComplete("landslide_size")

    dim_landslide_setting_check = Check(spark, CheckLevel.Error, "dim_landslide_setting") \
        .isComplete("setting_key") \
        .isUnique("setting_key") \
        .isComplete("landslide_setting")

    # Fact Checks with FK Validation
    fact_earthquake_check = Check(spark, CheckLevel.Error, "fact_earthquake") \
        .isComplete("id_earthquake") \
        .isUnique("id_earthquake") \
        .isComplete("date_key") \
        .isComplete("location_key") \
        .satisfies("Latitude BETWEEN -90 AND 90", "Valid Latitude") \
        .satisfies("Longitude BETWEEN -180 AND 180", "Valid Longitude") \
        .satisfies("date_key_valid = 1", "Date Key Missing in dim_date") \
        .satisfies("location_key_valid = 1", "Location Key Missing in dim_location")

    fact_volcano_check = Check(spark, CheckLevel.Error, "fact_volcano") \
        .isComplete("id_volcano") \
        .isUnique("id_volcano") \
        .isComplete("date_key") \
        .isComplete("location_key") \
        .isComplete("volcano_key") \
        .satisfies("latitude BETWEEN -90 AND 90", "Valid Latitude") \
        .satisfies("longitude BETWEEN -180 AND 180", "Valid Longitude") \
        .satisfies("date_key_valid = 1", "Date Key Missing in dim_date") \
        .satisfies("location_key_valid = 1", "Location Key Missing in dim_location") \
        .satisfies("volcano_key_valid = 1", "Volcano Key Missing in dim_volcano")

    fact_tsunami_check = Check(spark, CheckLevel.Error, "fact_tsunami") \
        .isComplete("id_tsunami") \
        .isUnique("id_tsunami") \
        .isComplete("date_key") \
        .isComplete("location_key") \
        .isComplete("validity_key") \
        .isComplete("cause_key") \
        .satisfies("date_key_valid = 1", "Date Key Missing in dim_date") \
        .satisfies("location_key_valid = 1", "Location Key Missing in dim_location") \
        .satisfies("validity_key_valid = 1", "Validity Key Missing in dim_tsunami_validity") \
        .satisfies("cause_key_valid = 1", "Cause Key Missing in dim_tsunami_cause")

    fact_landslide_check = Check(spark, CheckLevel.Error, "fact_landslide") \
        .isComplete("id_landslide") \
        .isUnique("id_landslide") \
        .isComplete("date_key") \
        .isComplete("location_key") \
        .isComplete("category_key") \
        .isComplete("trigger_key") \
        .isComplete("size_key") \
        .isComplete("setting_key") \
        .satisfies("latitude BETWEEN -90 AND 90", "Valid Latitude") \
        .satisfies("longitude BETWEEN -180 AND 180", "Valid Longitude") \
        .satisfies("date_key_valid = 1", "Date Key Missing in dim_date") \
        .satisfies("location_key_valid = 1", "Location Key Missing in dim_location") \
        .satisfies("category_key_valid = 1", "Category Key Missing in dim_landslide_category") \
        .satisfies("trigger_key_valid = 1", "Trigger Key Missing in dim_landslide_trigger") \
        .satisfies("size_key_valid = 1", "Size Key Missing in dim_landslide_size") \
        .satisfies("setting_key_valid = 1", "Setting Key Missing in dim_landslide_setting")

    # Run all checks
    results = [
        verificationSuite.onData(dim_date).addCheck(dim_date_check).run(),
        verificationSuite.onData(dim_location).addCheck(dim_location_check).run(),
        verificationSuite.onData(dim_volcano).addCheck(dim_volcano_check).run(),
        verificationSuite.onData(dim_tsunami_validity).addCheck(dim_tsunami_validity_check).run(),
        verificationSuite.onData(dim_tsunami_cause).addCheck(dim_tsunami_cause_check).run(),
        verificationSuite.onData(dim_landslide_category).addCheck(dim_landslide_category_check).run(),
        verificationSuite.onData(dim_landslide_trigger).addCheck(dim_landslide_trigger_check).run(),
        verificationSuite.onData(dim_landslide_size).addCheck(dim_landslide_size_check).run(),
        verificationSuite.onData(dim_landslide_setting).addCheck(dim_landslide_setting_check).run(),
        verificationSuite.onData(fact_earthquake_validated).addCheck(fact_earthquake_check).run(),
        verificationSuite.onData(fact_volcano_validated).addCheck(fact_volcano_check).run(),
        verificationSuite.onData(fact_tsunami_validated).addCheck(fact_tsunami_check).run(),
        verificationSuite.onData(fact_landslide_validated).addCheck(fact_landslide_check).run()
    ]

    # Combine results into a single DataFrame
    from pyspark.sql import DataFrame
    all_results_df = None
    for result in results:
        result_df = VerificationResult.checkResultsAsDataFrame(spark, result)  # Static call with spark and result
        if all_results_df is None:
            all_results_df = result_df
        else:
            all_results_df = all_results_df.union(result_df)

    if all_results_df.filter(col("constraint_status") != "Success").count() > 0:
        raise ValueError("Data quality check failed")
    

    logger.info('Tests are successfull')

    


except Exception as e:
    logger.exception(f"Data quality validation failed with error: {e}")

finally :
    logger.info("Spark session stopped")
    spark.stop()


    