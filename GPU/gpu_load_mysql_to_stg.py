from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Ingest MySQL to Hive (GPU-safe)") \
    .enableHiveSupport() \
    .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
    .config("spark.rapids.sql.enabled", "true") \
    .config("spark.executor.resource.gpu.amount", "1") \
    .config("spark.task.resource.gpu.amount", "0.25") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.files.openCostInBytes", "134217728") \
    .config("spark.sql.parquet.block.size", "134217728") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = (
    "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction"
    "?useSSL=false"
    "&serverTimezone=UTC"
    "&connectionTimeZone=Asia/Jakarta"
    "&autoReconnect=true"
    "&maxReconnects=10"
    "&socketTimeout=600000"
    "&connectTimeout=30000"
    "&tcpKeepAlive=true"
    "&useCursorFetch=true"
)

properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver",
    "fetchSize": "10000"
}

# Read from MySQL with partitioning using indexed column
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=16,
    properties=properties
)

# Optional transformation (GPU-accelerated if supported)
df_transformed = df.repartition(4).select(
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "currency",
    "status",
    "transaction_timestamp"
)

# Save to Hive staging table

df_transformed.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("stg.mobile_transactions_gpu")

spark.stop()