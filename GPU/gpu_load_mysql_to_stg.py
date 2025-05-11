from pyspark.sql import SparkSession
import ray
import pandas as pd

spark = SparkSession.builder \
    .appName("Load MySQL to Hive Staging with Ray") \
    .enableHiveSupport() \
    .getOrCreate()

ray.init(num_gpus=2)  # Removed setup_ray_cluster()

@ray.remote(num_gpus=1)
def process_partition(batch):
    # Mock processing step (e.g., GPU-based logic)
    return batch

jdbc_url = "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction?useSSL=false&serverTimezone=UTC&connectionTimeZone=Asia/Jakarta"
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=2,
    properties=properties
)

# Collect to Pandas batches (Ray processes Pandas)
batches = [row.asDict() for row in df.collect()]
futures = [process_partition.remote(pd.DataFrame([batch])) for batch in batches]
results = ray.get(futures)
final_df = spark.createDataFrame(pd.concat(results))

final_df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions_gpu")
spark.stop()
ray.shutdown()