from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum

# Start Spark Session with Hive support
spark = SparkSession.builder \
    .appName("RAPIDS GPU Aggregation") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from Hive (written by gpu_ingest_mysql.py)
df = spark.sql("SELECT * FROM stg.mobile_transactions_gpu")

# RAPIDS limitation: cast timestamp to UTC-compatible format
df = df.withColumn("ts", col("transaction_timestamp").cast("timestamp"))

# GPU-accelerated transformation: monthly summary
summary = df.withColumn("year", year("ts")) \
            .withColumn("month", month("ts")) \
            .groupBy("year", "month", "transaction_type", "currency") \
            .agg(sum("amount").alias("total_amount"))

# Optional: repartition before writing
summary = summary.repartition(4)

# Write aggregated data to datamart table
summary.write.mode("overwrite").format("parquet").saveAsTable("datamart.gpu_tx_summary")

spark.stop()