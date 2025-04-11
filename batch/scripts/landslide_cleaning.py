import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,col,when,split,regexp_replace , trim,to_timestamp, year, month, dayofmonth, hour, minute , desc

from batch.batchConfig.config import config
import logging

logger=logging.getLogger(__name__)




def fill_nulls_by_country(df, columns):
    for column in columns:
        # show rows with null in the current column
        null_rows = df.filter(col(column).isNull())
        #null_rows.show(n=df.count(), truncate=False)

        # get distinct countries with null in this column
        countries = null_rows.select("country_name").distinct().collect()
        countries = [row["country_name"] for row in countries if row["country_name"] is not None]

        # for each country find the most frequent value and fill null
        for country in countries:
            # fliter data for this country
            country_df = df.filter(col("country_name") == country)

            # get the most frequent value for the column in this country
            most_frequent = country_df.groupBy(column).count().orderBy(desc("count")).first()
            fill_value = most_frequent[column] if most_frequent and most_frequent[column] is not None else "unknown"

            # fill null with the most frequent value for this country
            df = df.withColumn(
                column,
                when(
                    (col("country_name") == country) & col(column).isNull(),
                    fill_value
                ).otherwise(col(column))
            )

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

    logger.info("Reading landslide data from minio")
    df_landslide=spark.read.csv(f"s3a://{config.bucket_name}/{config.bronze_prefix}global-landslide-data/Global_Landslide_Catalog_Export.csv",header=True,inferSchema=True)
    df_landslide = df_landslide.select(*config.selected_columns_ld)

    logger.info('cleaning the landslide dataset')

    df_landslide = df_landslide.filter(col("injury_count").cast("integer").isNotNull() | col("injury_count").isNull())

    df_landslide = df_landslide.filter(col("fatality_count").cast("integer").isNotNull() | col("fatality_count").isNull())

    df_landslide = df_landslide.filter(col("longitude").cast("float").isNotNull())

    df_landslide = df_landslide.dropDuplicates(['event_date', 'landslide_category', 'landslide_trigger', 'landslide_size','landslide_setting', 'fatality_count', 'injury_count', 'country_name',  'longitude', 'latitude'])

    df_landslide = df_landslide.withColumn("event_date", to_timestamp(col("event_date"), "MM/dd/yyyy hh:mm:ss a")) \
                           .withColumn("Year", year(col("event_date"))) \
                           .withColumn("Month", month(col("event_date"))) \
                           .withColumn("Day", dayofmonth(col("event_date"))) \
                           .withColumn("Hour", hour(col("event_date"))) \
                           .withColumn("Minute", minute(col("event_date"))) \
                           .drop("event_date")
    


    df_landslide = df_landslide.withColumn("country_name",
                                           when(col("country_name").isNull() & (col("longitude").between(-45, -43)) & (col("latitude").between(-23, -20)), "Brazil")  # Brazil (e.g., -22.912451, -44.302371; -22.527652, -43.212778; -20.23931048, -43.42891666)
                                           .when(col("country_name").isNull() & (col("longitude").between(-169, -66)) & (col("latitude").between(16, 71)), "United States")  # US
                                           .when(col("country_name").isNull() & (col("longitude").between(-135, -65)) & (col("latitude").between(-42, -6)), "Brazil (Other)")  # Other Brazil regions
                                           .when(col("country_name").isNull() & (col("longitude").between(-13, 11)) & (col("latitude").between(49, 62)), "United Kingdom")  # UK
                                           .when(col("country_name").isNull() & (col("longitude").between(67, 135)) & (col("latitude").between(3, 36)), "India")  # India
                                           .when(col("country_name").isNull() & (col("longitude").between(92, 161)) & (col("latitude").between(-12, 13)), "Indonesia")  # Indonesia
                                           .when(col("country_name").isNull() & (col("longitude").between(-100, -87)) & (col("latitude").between(14, 20)), "Mexico")  # Mexico
                                           .when(col("country_name").isNull() & (col("longitude").between(169, 179)) & (col("latitude").between(-44, -34)), "New Zealand")  # New Zealand
                                           .when(col("country_name").isNull() & (col("longitude").between(103, 130)) & (col("latitude").between(27, 54)), "China")  # China
                                           .when(col("country_name").isNull() & (col("longitude").between(118, 125)) & (col("latitude").between(4, 11)), "Philippines")  # Philippines
                                           .when(col("country_name").isNull() & (col("longitude").between(-61, -60)) & (col("latitude").between(10, 14)), "Trinidad and Tobago")  # Trinidad
                                           .when(col("country_name").isNull() & (col("longitude").between(-85, -75)) & (col("latitude").between(-5, 5)), "Colombia")  # Colombia
                                           .when(col("country_name").isNull() & (col("longitude").between(-80, -65)) & (col("latitude").between(-18, 15)), "Peru")  # Peru
                                           .when(col("country_name").isNull() & (col("longitude").between(110, 155)) & (col("latitude").between(-11, -1)), "Papua New Guinea")  # PNG
                                           .when(col("country_name").isNull() & (col("longitude").between(139, 154)) & (col("latitude").between(33, 45)), "Japan")  # Japan
                                           .when(col("country_name").isNull() & (col("longitude").between(6, 30)) & (col("latitude").between(35, 47)), "Italy")  # Italy
                                           .when(col("country_name").isNull() & (col("longitude").between(-82, -77)) & (col("latitude").between(17, 23)), "Jamaica")  # Jamaica
                                           .when(col("country_name").isNull() & (col("longitude").between(27, 45)) & (col("latitude").between(-5, 5)), "Kenya")  # Kenya
                                           .when(col("country_name").isNull() & (col("longitude").between(112, 130)) & (col("latitude").between(-1, 8)), "Malaysia")  # Malaysia
                                           .when(col("country_name").isNull() & (col("longitude").between(178, 180)) & (col("latitude").between(-19, -17)), "Fiji")  # Fiji
                                           .when(col("country_name").isNull() & (col("longitude").between(28, 48)) & (col("latitude").between(35, 45)), "Turkey")  # Turkey
                                           .when(col("country_name").isNull() & (col("longitude").between(-75, -65)) & (col("latitude").between(-56, -17)), "Argentina")  # Argentina
                                           .when(col("country_name").isNull() & (col("longitude").between(16, 32)) & (col("latitude").between(-35, -20)), "South Africa")  # South Africa
                                           .when(col("country_name").isNull() & (col("longitude").between(141, 154)) & (col("latitude").between(-39, -10)), "Australia")  # Australia
                                           .when(col("country_name").isNull() & (col("longitude").between(-141, -52)) & (col("latitude").between(41, 83)), "Canada")  # Canada
                                           .when(col("country_name").isNull() & (col("longitude").between(-90, -85)) & (col("latitude").between(13, 17)), "Guatemala")  # Guatemala
                                           .when(col("country_name").isNull() & (col("longitude").between(22, 38)) & (col("latitude").between(22, 31)), "Egypt")  # Egypt
                                           .when(col("country_name").isNull() & (col("longitude").between(60, 80)) & (col("latitude").between(35, 45)), "Tajikistan")  # Tajikistan
                                           .when(col("country_name").isNull() & (col("longitude").between(2, 14)) & (col("latitude").between(4, 14)), "Nigeria")  # Nigeria
                                           .when(col("country_name").isNull() & (col("longitude").between(-15, -1)) & (col("latitude").between(5, 15)), "Sierra Leone")  # Sierra Leone
                                           .when(col("country_name").isNull() & (col("longitude").between(25, 35)) & (col("latitude").between(-15, -5)), "Malawi")  # Malawi
                                           .when(col("country_name").isNull() & (col("longitude").between(-5, 5)) & (col("latitude").between(35, 45)), "Algeria")  # Algeria
                                           .when(col("country_name").isNull() & (col("longitude").between(20, 40)) & (col("latitude").between(46, 52)), "Ukraine")  # Ukraine
                                           .when(col("country_name").isNull() & (col("longitude").between(34, 42)) & (col("latitude").between(31, 37)), "Jordan")  # Jordan
                                           .when(col("country_name").isNull() & (col("longitude").between(-150, -140)) & (col("latitude").between(-18, -15)), "French Polynesia")  # French Polynesia
                                           .when(col("country_name").isNull() & (col("longitude").between(48, 52)) & (col("latitude").between(35, 40)), "Iran")  # Iran
                                           .when(col("country_name").isNull() & (col("longitude").between(65, 67)) & (col("latitude").between(24, 25)), "Pakistan")  # Pakistan
                                           .when(col("country_name").isNull() & (col("longitude").between(36, 39)) & (col("latitude").between(7, 9)), "Ethiopia")  # Ethiopia
                                           .when(col("country_name").isNull() & (col("longitude").between(-10, -8)) & (col("latitude").between(38, 40)), "Portugal")  # Portugal
                                           .when(col("country_name").isNull() & (col("longitude").between(47, 49)) & (col("latitude").between(-20, -19)), "Madagascar")  # Madagascar
                                           .when(col("country_name").isNull() & (col("longitude").between(-39, -38)) & (col("latitude").between(-13, -12)), "Ocean (near Brazil)")  # Ocean
                                           .when(col("country_name").isNull() & (col("longitude").between(35, 37)) & (col("latitude").between(-8, -7)), "Ocean (near Tanzania)")  # Ocean
                                           .when(col("country_name").isNull() & (col("longitude").between(-171, -170)) & (col("latitude").between(-15, -14)), "American Samoa")  # American Samoa
                                           .when(col("country_name").isNull() & (col("longitude").between(-79, -78)) & (col("latitude").between(7, 8)), "Panama")  # Panama
                                           .when(col("country_name").isNull() & (col("longitude").between(-69, -68)) & (col("latitude").between(-17, -16)), "Bolivia")  # Bolivia
                                           .otherwise(col("country_name")))
    

    df_landslide = fill_nulls_by_country(df_landslide, config.columns_to_fill_ld)
    



    logger.info('the landslide dataset is cleaned')


    logger.info("Creating namespace nessie.silver if it doesn't exist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    logger.info("Writing landslide data to Iceberg table nessie.silver.df_landslide")

    df_landslide.write.format("iceberg").mode("overwrite").saveAsTable("nessie.silver.df_landslide")
    logger.info("landslide data successfully written to Iceberg table")


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


    