import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,col,when,split
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

    logger.info("Reading earthquake data from minio")
    df_eartqueke = spark.read.option("maxColumns", "20481").option("header", "true").option("delimiter", ";").csv(f"s3a://{config.bucket_name}/{config.bronze_prefix}earthquakes/significant-earthquake-database.csv", inferSchema=True)
    df_eartqueke=df_eartqueke.select("ID Earthquake", "Year", "Month", "Day", "EQ Primary", "Country",  "Earthquake : Deaths", "Earthquake : Deaths Description", "Earthquake : Damage Description", "Coordinates")
    df_eartqueke=df_eartqueke.filter(col('Year')>0)
    logger.info("Initial data selection and filtering by Year > 0 completed")

    df_eartqueke = df_eartqueke.withColumn(
    "Coordinates",
    when((col("ID Earthquake") == 7261) & (col("Year") == 1118), "51.5, -2.5")  # UK
    .when((col("ID Earthquake") == 8261) & (col("Year") == 1842), "46.8, -71.2")  # Canada
    .when((col("ID Earthquake") == 6266) & (col("Year") == 1916), "10.0, -84.1")  # Costa Rica
    .when((col("ID Earthquake") == 5948) & (col("Year") == 1489), "40.7, 27.5")  # Turkey
    .when((col("ID Earthquake") == 6881) & (col("Year") == 1719), "13.7, -89.1")  # El Salvador
    .when((col("ID Earthquake") == 9892) & (col("Year") == 1681), "-6.9, 107.6")  # Indonesia
    .when((col("ID Earthquake") == 7264) & (col("Year") == 1732), "64.0, -20.3")  # Iceland
    .when((col("ID Earthquake") == 6882) & (col("Year") == 1798), "13.8, -89.5")  # El Salvador
    .when((col("ID Earthquake") == 8179) & (col("Year") == 857), "31.0, 31.2")  # Egypt
    .when((col("ID Earthquake") == 338) & (col("Year") == 1048), "51.9, -1.3")  # UK
    .when((col("ID Earthquake") == 7263) & (col("Year") == 1734), "64.1, -21.0")  # Iceland
    .when((col("ID Earthquake") == 6015) & (col("Year") == 1403), "33.5, 36.3")  # Syria
    .when((col("ID Earthquake") == 7787) & (col("Year") == 1870), "45.8, 15.9")  # Croatia
    .when((col("ID Earthquake") == 10061) & (col("Year") == 1865), "34.7, 135.5")  # Japan
    .when((col("ID Earthquake") == 6554) & (col("Year") == 1903), "24.6, 121.5")  # Taiwan
    .when((col("ID Earthquake") == 6639) & (col("Year") == 1932), "37.2, -117.0")  # USA
    .when((col("ID Earthquake") == 6880) & (col("Year") == 1707), "13.6, -89.2")  # El Salvador
    .when((col("ID Earthquake") == 7262) & (col("Year") == 1734), "55.0, -2.0")  # UK
    .when((col("ID Earthquake") == 6379) & (col("Year") == 1957), "-9.5, 159.5")  # Solomon Islands
    .when((col("ID Earthquake") == 10012) & (col("Year") == 1848), "-41.3, 174.8")  # New Zealand
    .when((col("ID Earthquake") == 1624) & (col("Year") == 1817), "34.6, 109.0")  # China
    .when((col("ID Earthquake") == 7790) & (col("Year") == 1870), "40.8, 14.1")  # Italy
    .when((col("ID Earthquake") == 6306) & (col("Year") == 1878), "37.7, -122.5")  # USA
    .when((col("ID Earthquake") == 6067) & (col("Year") == 1852), "36.6, -121.9")  # USA
    .when((col("ID Earthquake") == 9952) & (col("Year") == 1269), "38.0, 38.0")  # Turkey
    .when((col("ID Earthquake") == 9912) & (col("Year") == 1885), "36.9, 5.2")  # Algeria
    .when((col("ID Earthquake") == 10214) & (col("Year") == 1919), "10.5, -67.0")  # Venezuela
    .when((col("ID Earthquake") == 7775) & (col("Year") == 1978), "40.8, 15.0")  # Italy
    .when((col("ID Earthquake") == 1268) & (col("Year") == 1737), "22.5, 88.4")  # India
    .when((col("ID Earthquake") == 10009) & (col("Year") == 1881), "-21.1, -175.2")  # Tonga
    .when((col("ID Earthquake") == 3800) & (col("Year") == 1945), "1.2, 31.6")  # Uganda
    .when((col("ID Earthquake") == 1056) & (col("Year") == 1669), "24.8, 92.0")  # India
    .when((col("ID Earthquake") == 6612) & (col("Year") == 1885), "-5.3, 102.2")  # Indonesia
    .when((col("ID Earthquake") == 9800) & (col("Year") == 1651), "-16.5, -73.6")  # Peru
    .when((col("ID Earthquake") == 6879) & (col("Year") == 1659), "13.7, -89.3")  # El Salvador
    .when((col("ID Earthquake") == 6076) & (col("Year") == 1839), "35.0, -90.0")  # USA
    .when((col("ID Earthquake") == 3567) & (col("Year") == 1935), "-0.6, 133.0")  # Indonesia
    .when((col("ID Earthquake") == 9790) & (col("Year") == 1537), "19.3, -98.9")  # Mexico
    .when((col("ID Earthquake") == 10210) & (col("Year") == 1870), "16.4, -61.7")  # Guadeloupe
    .when((col("ID Earthquake") == 6016) & (col("Year") == 1332), "37.9, 27.3")  # Turkey
    .when((col("ID Earthquake") == 10062) & (col("Year") == 1865), "-16.1, -74.6")  # Peru
    .when((col("ID Earthquake") == 10154) & (col("Year") == 1866), "38.4, 22.4")  # Greece
    .when((col("ID Earthquake") == 6577) & (col("Year") == 1912), "6.9, 158.2")  # Micronesia
    .when((col("ID Earthquake") == 3651) & (col("Year") == 1939), "-0.5, 100.3")  # Indonesia
    .when((col("ID Earthquake") == 10060) & (col("Year") == 1876), "35.7, 139.7")  # Japan
    .when((col("ID Earthquake") == 10058) & (col("Year") == 551), "34.0, 25.0")  # Greece
    .when((col("ID Earthquake") == 6878) & (col("Year") == 1576), "13.5, -89.6")  # El Salvador
    .when((col("ID Earthquake") == 6697) & (col("Year") == 1500), "36.0, -120.0")  # USA
    .when((col("ID Earthquake") == 10023) & (col("Year") == 1877), "40.8, -124.0")  # USA
    .when((col("ID Earthquake") == 3705) & (col("Year") == 1942), "-8.1, 114.5")  # Indonesia
    .when((col("ID Earthquake") == 9914) & (col("Year") == 1885), "65.0, -20.0")  # Iceland
    .when((col("ID Earthquake") == 10053) & (col("Year") == 1897), "-39.0, 176.0")  # New Zealand
    .when((col("ID Earthquake") == 7791) & (col("Year") == 1871), "44.5, 11.0")  # Italy
    .otherwise(col("Coordinates")))

    df_eartqueke = df_eartqueke.withColumn("Latitude", split(col("Coordinates"), ", ")[0].cast("float")).withColumn("Longitude", split(col("Coordinates"), ", ")[1].cast("float")).drop("Coordinates")

    df_eartqueke = df_eartqueke.filter(~((col("Month").isNull() | col("Day").isNull()) & col("Earthquake : Deaths").isNull()))
    logger.info("Filtered out rows with null Month/Day and Deaths")

    # Lookup table as dataframe
    dates = spark.createDataFrame([
        (411, 8, 12), (513, 7, 20), (2771, 5, 15), (234, 1, 25), (445, 5, 17),
        (999, 11, 13), (1374, 4, 10), (8099, 5, 15), (1806, 1, 1), (430, 5, 11),
        (465, 8, 14), (8116, 8, 22), (402, 9, 30), (218, 6, 15), (376, 2, 18),
        (405, 4, 23), (455, 3, 9), (397, 10, 3), (485, 8, 1), (516, 11, 5),
        (83, 6, 12), (637, 12, 10), (321, 7, 14), (525, 10, 25), (4019, 4, 18),
        (416, 8, 29), (600, 10, 15), (927, 1, 20), (1864, 2, 4), (2066, 6, 30),
        (419, 6, 29), (163, 4, 18), (1001, 3, 28), (1047, 11, 25), (185, 8, 10),
        (187, 4, 19), (9952, 9, 1), (1108, 1, 11), (155, 5, 23), (227, 12, 6),
        (552, 2, 14), (615, 11, 18), (989, 2, 15), (199, 8, 20), (211, 5, 24),
        (309, 1, 8), (546, 3, 15), (7964, 6, 19), (944, 2, 23), (81, 10, 11),
        (9869, 7, 6), (116, 9, 14), (370, 8, 5), (418, 5, 22), (1418, 2, 17),
        (1447, 4, 30), (365, 6, 10), (655, 9, 25), (737, 3, 12), (2079, 12, 21),
        (344, 10, 23), (590, 5, 8), (8181, 7, 19), (1027, 8, 17), (1325, 11, 25),
        (188, 7, 27), (761, 11, 16), (220, 12, 15), (646, 10, 20), (1203, 5, 3),
        (8117, 4, 25), (74, 9, 10), (326, 12, 24), (791, 11, 20), (408, 10, 26),
        (1014, 3, 15), (148, 12, 14), (429, 9, 5), (566, 8, 20), (616, 12, 18),
        (2287, 8, 15), (462, 12, 25), (1353, 1, 30)], ["ID Earthquake", "Month_fill", "Day_fill"])
    
    logger.info("Created lookup table for missing dates")

    # Join and fill nulls
    df_eartqueke = df_eartqueke.join(dates, "ID Earthquake", "left").withColumn("Month", coalesce(col("Month"), col("Month_fill"))).withColumn("Day", coalesce(col("Day"), col("Day_fill"))).drop("Month_fill", "Day_fill")
    logger.info("Joined data with lookup table and filled missing Month/Day values")

    logger.info("Creating namespace nessie.silver if it doesn't exist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    logger.info("Writing earthquake data to Iceberg table nessie.silver.df_earthquake")

    df_eartqueke.write.format("iceberg").mode("overwrite").saveAsTable("nessie.silver.df_earthquake")
    logger.info("Earthquake data successfully written to Iceberg table")


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


    