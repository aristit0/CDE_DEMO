from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Load MySQL to Hive Staging") \
    .enableHiveSupport() \
    .getOrCreate()

# Set block size to 128MB
spark.conf.set("parquet.block.size", 134217728)  # 128 * 1024 * 1024
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")  # Optional but efficient

jdbc_url = "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction?useSSL=false&serverTimezone=UTC&connectionTimeZone=Asia/Jakarta"
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Optimized JDBC read using partitioned read (parallel)
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",    # Must be numeric & indexed
    lowerBound=1,
    upperBound=1000000000,      # Total row estimate or max ID
    numPartitions=80,           # Slightly more than total cores
    properties=properties
)

# Repartition before writing to target 128MB file size (based on data volume)
df = df.repartition(80)

# Write to Hive (Parquet, external or managed table depending on Hive setup)
df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions")

spark.stop()