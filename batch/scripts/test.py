import pyspark
from pyspark.sql import SparkSession

WAREHOUSE = "s3a://warehouse/"
AWS_ACCESS_KEY = "ali"
AWS_SECRET_KEY = "aliali123"
AWS_S3_ENDPOINT = "http://minio:9000"
NESSIE_URI = "http://nessie:19120/api/v1"

print(f"AWS_S3_ENDPOINT: {AWS_S3_ENDPOINT}")
print(f"NESSIE_URI: {NESSIE_URI}")
print(f"WAREHOUSE: {WAREHOUSE}")

conf = (
    pyspark.SparkConf()
    .setAppName("IcebergNessieMinio")
    .set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4")
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
    .set("spark.sql.catalog.nessie.ref", "main")
    .set("spark.sql.catalog.nessie.authentication.type", "NONE")
    .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")  # Switch to HadoopFileIO
    .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .set("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .set("spark.sql.defaultCatalog", "nessie")
)

# Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# Debug: Check current catalog and schema
print("Current catalog:", spark.conf.get("spark.sql.defaultCatalog"))
print("Current schema:", spark.sql("SELECT current_schema()").collect()[0][0])

# Create namespace and table explicitly
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.default")
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.default.test_table (
        name STRING,
        age INT
    ) USING iceberg
""")

# Create a simple DataFrame
data = [("Ali", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["name", "age"])

# Write to Iceberg table
df.write.format("iceberg").mode("overwrite").save("nessie.default.test_table")

# Read back to verify
df_read = spark.read.format("iceberg").load("nessie.default.test_table")
df_read.show()

# Stop Spark
spark.stop()