from pyspark.sql import SparkSession

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Ingest MySQL to Hive (GPU-safe)") \
    .enableHiveSupport() \
    .getOrCreate()

# JDBC connection string
jdbc_url = (
    "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction"
    "?useSSL=false"
    "&serverTimezone=UTC"
    "&connectionTimeZone=Asia/Jakarta"
)

# JDBC connection properties
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver",
    "autoReconnect": "true",
    "connectTimeout": "30000",        # 30s
    "socketTimeout": "600000",        # 10 min
    "tcpKeepAlive": "true",
    "interactiveClient": "true",
    "maxReconnects": "10"
}

# Parallel JDBC read using indexed transaction_id
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=4,  # Adjust based on cluster resources
    properties=properties
)

# Optional: repartition for file layout optimization (128 MB target)
df = df.repartition(8)

# Write to Hive staging table (external or managed Parquet table)
df.write.mode("overwrite") \
    .format("parquet") \
    .saveAsTable("stg.mobile_transactions_gpu")

# Stop session
spark.stop()