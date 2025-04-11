import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,col,when,split,regexp_replace , trim,to_timestamp, year, month, dayofmonth, hour, minute , desc

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

    logger.info("Reading volcano data from minio")
    df_volcano=spark.read.csv(f"s3a://{config.bucket_name}/{config.bronze_prefix}significant-volcanic-eruption-database/significant-volcanic-eruption-database.csv",header=True,inferSchema=True, sep=';')
    df_volcano = df_volcano.select(*config.selected_columns_v)

    logger.info('cleaning the volcano dataset')

    df_volcano=df_volcano.filter(col("Year") >0).orderBy('Year')

    df_volcano = df_volcano.dropDuplicates(['Year', 'Month', 'Day', 'Volcano Name', 'Country', 'Elevation','Volcano Type', 'Status', 'Volcanic Explosivity Index','Volcano : Deaths Description', 'Total Effects : Deaths', 'Coordinates'])

    # Dictionary of eruption dates
    eruption_dates = {
        ("Tungurahua", 2010, "Ecuador"): (11, 28),
        ("Grimsvotn", 1684, "Iceland"): (12, 15),
        ("Sinarka", 1872, "Russia"): (11, 10),
        ("Kelut", 1311, "Indonesia"): (5, 15),
        ("Tangkubanparahu", 1923, "Indonesia"): (6, 20),
        ("Camiguin", 1954, "Philippines"): (8, 31),
        ("Rabaul", 1878, "Papua New Guinea"): (1, 20),
        ("Oshima", 1957, "Japan"): (8, 15),
        ("Merapi", 1932, "Indonesia"): (2, 7),
        ("Tengchong", 1609, "China"): (5, 1),
        ("Grimsvotn", 1860, "Iceland"): (5, 26),
        ("Asuncion", 1819, "United States"): (10, 1),
        ("Karthala", 1883, "Comoros"): (3, 15),
        ("Kelut", 1586, "Indonesia"): (10, 5),
        ("Krakatau", 416, "Indonesia"): (8, 1),
        ("Lamongan", 1869, "Indonesia"): (8, 10),
        ("Raung", 1817, "Indonesia"): (7, 15),
        ("Katla", 934, "Iceland"): (10, 1),
        ("Aso", 1872, "Japan"): (12, 10),
        ("Pacaya", 1565, "Guatemala"): (8, 20),
        ("Katla", 1357, "Iceland"): (6, 1),
        ("Changbaishan", 1000, "North Korea"): (4, 1),
        ("Dukono", 1550, "Indonesia"): (11, 15),
        ("Camiguin", 1949, "Philippines"): (6, 30),
        ("Gamalama", 1775, "Indonesia"): (9, 1),
        ("Mayon", 1858, "Philippines"): (1, 15),
        ("Ceboruco", 930, "Mexico"): (2, 1),
        ("Kavachi", 1951, "Solomon Is."): (3, 15),
        ("Raung", 1730, "Indonesia"): (6, 1),
        ("Grimsvotn", 1784, "Iceland"): (4, 8),
        ("Dieng Volc Complex", 1786, "Indonesia"): (6, 20),
        ("Ambrym", 50, "Vanuatu"): (7, 1),
        ("Taupo", 230, "New Zealand"): (3, 1),
        ("Unnamed", 1907, "Tonga"): (7, 15),
        ("Augustine", 350, "United States"): (9, 1),
        ("Fuego", 1617, "Guatemala"): (10, 1),
        ("Billy Mitchell", 1580, "Papua New Guinea"): (5, 1),
        ("Raung", 1593, "Indonesia"): (7, 10),
        ("Semeru", 1946, "Indonesia"): (2, 6),
        ("Vilyuchik", 1981, "Russia"): (8, 1),
        ("Uwayrid, Harrat", 640, "Saudi Arabia"): (5, 1),
        ("Calbuco", 1917, "Chile"): (4, 11),
        ("Chichon, El", 1375, "Mexico"): (3, 1),
        ("Katla", 1177, "Iceland"): (9, 1),
        ("Oshima", 1789, "Japan"): (7, 1),
        ("Akita-Yake-yama", 1988, "Japan"): (5, 1),
        ("Kelut", 1334, "Indonesia"): (6, 1),
        ("Sakura-jima", 1946, "Japan"): (1, 29),
        ("Ambrym", 1894, "Vanuatu"): (10, 15),
        ("Semeru", 1992, "Indonesia"): (7, 1),
        ("Pago", 710, "Papua New Guinea"): (8, 1),
        ("Lamongan", 1843, "Indonesia"): (8, 5),
        ("Rabaul", 683, "Papua New Guinea"): (6, 1),
        ("Chokai", 1801, "Japan"): (7, 15),
        ("Cotopaxi", 1698, "Ecuador"): (8, 1),
        ("Aso", 1854, "Japan"): (2, 15),
        ("Makian", 1760, "Indonesia"): (7, 25),
        ("Iliwerung", 1870, "Indonesia"): (8, 1),
        ("Manam", 1899, "Papua New Guinea"): (6, 1),
        ("Manam", 2007, "Papua New Guinea"): (3, 27),
        ("Quilotoa", 1280, "Ecuador"): (10, 1),
        ("Sirung", 1953, "Indonesia"): (6, 15),
        ("Aso", 1957, "Japan"): (10, 20),
        ("Tangkubanparahu", 1978, "Indonesia"): (3, 1),
        ("Banda Api", 1598, "Pacific Ocean"): (5, 1),
        ("Hood", 1934, "United States"): (5, 1),
        ("Merapi", 1902, "Indonesia"): (12, 15),
        ("Shikotsu", 1804, "Japan"): (8, 1),
        ("Ischia", 1302, "Italy"): (2, 1),
        ("Katla", 950, "Iceland"): (6, 1),
        ("Unzen", 1957, "Japan"): (7, 15),
        ("Aso", 1485, "Japan"): (1, 20),
        ("Tullu Moje", 1900, "Ethiopia"): (4, 1),
        ("Kilauea", 1823, "United States"): (2, 15),
        ("Mutnovsky", 1991, "Russia"): (6, 15),
        ("Tinakula", 1840, "Solomon Is."): (8, 1),
        ("Aso", 1331, "Japan"): (12, 10),
        ("Santorini", 1050, "Greece"): (7, 1),
        ("Savo", 1840, "Solomon Is."): (8, 1),
        ("Raung", 1638, "Indonesia"): (8, 1),
        ("San Salvador", 590, "El Salvador"): (6, 1),
        ("Tungurahua", 2010, "Ecuador"): (1, 5),
        ("Katla", 1262, "Iceland"): (8, 1),
        ("Peuet Sague", 1837, "Indonesia"): (9, 15),
        ("Tungurahua", 1640, "Ecuador"): (10, 1),
        ("Paluweh", 1973, "Indonesia"): (1, 15),
        ("Etna", 1329, "Italy"): (7, 15),
        ("Arhab, Harra of", 500, "Yemen"): (6, 1),
        ("Merapi", 1587, "Indonesia"): (8, 1),
        ("Aoba", 1914, "Vanuatu"): (7, 1),
        ("Merapi", 2000, "Indonesia"): (11, 10),
        ("Dakataua", 653, "Papua New Guinea"): (7, 1),
        ("Merapi", 1962, "Indonesia"): (10, 15),
        ("Grimsvotn", 1861, "Iceland"): (5, 15),
        ("Kuwae", 1430, "Vanuatu"): (2, 1),
        ("Karthala", 1903, "Comoros"): (8, 1),
        ("Krakatau", 1884, "Indonesia"): (2, 1),
        ("Rabaul", 1850, "Papua New Guinea"): (6, 1),
        ("Tungurahua", 2011, "Ecuador"): (4, 11),
        ("Ilopango", 450, "El Salvador"): (8, 1),
        ("Sakura-jima", 764, "Japan"): (8, 1),
        ("Ksudach", 240, "Russia"): (3, 1),
        ("Balbi", 1825, "Papua New Guinea"): (7, 1),
        ("Paluweh", 2013, "Indonesia"): (2, 5),
        ("Kilauea", 1790, "United States"): (11, 1),
        ("Mayon", 1928, "Philippines"): (1, 15),
        ("Guntur", 1690, "Indonesia"): (6, 1),
        ("Santorini", 1707, "Greece"): (5, 15),
        ("Banda Api", 1615, "Pacific Ocean"): (3, 15),
        ("Aoba", 1670, "Vanuatu"): (8, 1),
        ("Victory", 1890, "Papua New Guinea"): (6, 1),
        ("Okataina", 2015, "New Zealand"): (10, 15),
        ("Fentale", 1250, "Ethiopia"): (8, 1),
        ("Semeru", 1860, "Indonesia"): (4, 15),
        ("Hualalai", 1784, "United States"): (7, 1),
        ("Iwo-Tori-shima", 1664, "Japan"): (8, 1),
        ("Kieyo", 1800, "Tanzania"): (6, 1),
        ("Katla", 920, "Iceland"): (10, 1),
        ("Kliuchevskoi", 1762, "Russia"): (8, 1),
        ("Vesuvius", 1698, "Italy"): (5, 15),
        ("Vesuvius", 1813, "Italy"): (5, 15),
        ("Kelut", 1998, "Indonesia"): (7, 15),
        ("Hualalai", 1800, "United States"): (8, 1),
        ("Teon", 1660, "Pacific Ocean"): (2, 15),
        ("Bagana", 1883, "Papua New Guinea"): (12, 15),
        ("Marapi", 1975, "Indonesia"): (1, 15),
        ("Kelut", 1385, "Indonesia"): (6, 1),
        ("Stromboli", 1954, "Italy"): (2, 15),
        ("St. Helens", 1800, "United States"): (8, 1),
        ("Semeru", 1909, "Indonesia"): (9, 15),
        ("Sabancaya", 1784, "Peru"): (7, 15),
        ("Bona-Churchill", 847, "United States"): (6, 1),
        ("Kuchinoerabu-jima", 1841, "Japan"): (4, 15),
        ("Aoba", 1870, "Vanuatu"): (8, 1),
        ("Kelut", 1376, "Indonesia"): (6, 1),
        ("Augustine", 1540, "United States"): (8, 1),
        ("Semeru", 2002, "Indonesia"): (7, 15),
        ("Asama", 1936, "Japan"): (2, 15),
        ("Raikoke", 1778, "Russia"): (8, 15),
        ("Katla", 1500, "Iceland"): (6, 1),
        ("Mayon", 1875, "Philippines"): (8, 1),
        ("Aso", 1826, "Japan"): (10, 15),
        ("Dakataua", 1895, "Papua New Guinea"): (8, 1),
        ("Colima", 1576, "Mexico"): (8, 1),
        ("Krisuvik", 1151, "Iceland"): (6, 1),
        ("Savo", 1568, "Solomon Is."): (8, 1),
        ("Santorini", 46, "Greece"): (6, 1),
        ("Arhab, Harra of", 200, "Yemen"): (6, 1),
        ("Bona-Churchill", 60, "United States"): (6, 1),
        ("Lokon-Empung", 1775, "Indonesia"): (8, 1),
        ("Tseax River Cone", 1730, "Canada"): (6, 1),
        ("Ambalatungan Group", 1952, "Philippines"): (8, 1),
        ("Mombacho", 1570, "Nicaragua"): (8, 1),
        ("Grimsvotn", 1629, "Iceland"): (8, 1),
        ("Cereme", 1698, "Indonesia"): (8, 1),
        ("Vesuvius", 787, "Italy"): (8, 1),
        ("Ruang", 1870, "Indonesia"): (8, 15),
        ("Guntur", 1829, "Indonesia"): (8, 1),
        ("Bardarbunga", 1477, "Iceland"): (2, 15),
        ("Vesuvius", 1873, "Italy"): (8, 1),
        ("Long Island", 1660, "Papua New Guinea"): (8, 1),
        ("Raung", 1597, "Indonesia"): (8, 1)}


    for (volcano, year, country), (month, day) in eruption_dates.items():
        df_volcano = df_volcano.withColumn(
            "Month",
            when((col("Volcano Name") == volcano) & (col("Year") == year) & (col("Country") == country) & col("Month").isNull(), month)
            .otherwise(col("Month"))
        ).withColumn(
            "Day",
            when((col("Volcano Name") == volcano) & (col("Year") == year) & (col("Country") == country) & col("Day").isNull(), day)
            .otherwise(col("Day")))
        
    

    df_volcano = df_volcano.withColumn("latitude", split(col("Coordinates"), ",").getItem(0)).withColumn("longitude", split(col("Coordinates"), ",").getItem(1)).drop("Coordinates")

    df_volcano = df_volcano.withColumn("Volcanic Explosivity Index",when(col("Volcanic Explosivity Index") == 0, "Non-explosive")
                                       .when(col("Volcanic Explosivity Index") == 1, "Gentle")
                                       .when(col("Volcanic Explosivity Index") == 2, "Explosive")
                                       .when(col("Volcanic Explosivity Index") == 3, "Severe")
                                       .when(col("Volcanic Explosivity Index") == 4, "Cataclysmic")
                                       .when(col("Volcanic Explosivity Index") == 5, "Paroxysmal")
                                       .when(col("Volcanic Explosivity Index") == 6, "Colossal")
                                       .when(col("Volcanic Explosivity Index") == 7, "Super-colossal")
                                       .when(col("Volcanic Explosivity Index") == 8, "Mega-colossal")
                                       .otherwise(col("Volcanic Explosivity Index")))
    



    logger.info('the volcano dataset is cleaned')


    logger.info("Creating namespace nessie.silver if it doesn't exist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    
    logger.info("Writing volcano data to Iceberg table nessie.silver.df_volcano")

    df_volcano.write.format("iceberg").mode("overwrite").saveAsTable("nessie.silver.df_volcano")
    logger.info("volcano data successfully written to Iceberg table")


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


    