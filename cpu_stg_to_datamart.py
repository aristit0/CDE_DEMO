from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum

spark = SparkSession.builder.appName("Transform to Datamart").enableHiveSupport().getOrCreate()

df = spark.sql("SELECT * FROM stg.mobile_transactions")

# Example transformation: monthly transaction summary
summary = df.withColumn("year", year("transaction_timestamp")) \
            .withColumn("month", month("transaction_timestamp")) \
            .groupBy("year", "month", "transaction_type", "currency") \
            .agg(sum("amount").alias("total_amount"))

summary.write.mode("overwrite").format("parquet").saveAsTable("datamart.mobile_tx_summary")
spark.stop()