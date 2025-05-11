from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum

spark = SparkSession.builder \
    .appName("Transform to Datamart") \
    .enableHiveSupport() \
    .getOrCreate()

# Tune for 128MB output file size
spark.conf.set("parquet.block.size", 134217728)
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Read from staging
df = spark.sql("SELECT * FROM stg.mobile_transactions")

# Transformation: Monthly aggregated transaction summary
summary = df.withColumn("year", year("transaction_timestamp")) \
            .withColumn("month", month("transaction_timestamp")) \
            .groupBy("year", "month", "transaction_type", "currency") \
            .agg(sum("amount").alias("total_amount"))

# Repartition to control number of files (based on data volume; adjust as needed)
summary = summary.repartition(40)

# Write to datamart schema
summary.write.mode("overwrite").format("parquet").saveAsTable("datamart.mobile_tx_summary")

spark.stop()