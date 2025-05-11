from pyspark.sql import SparkSession
import ray
from ray.util.spark import setup_ray_cluster
from pyspark.sql.functions import col
import pandas as pd

# Start Spark session
spark = SparkSession.builder \
    .appName("Load MySQL to Hive Staging with Ray") \
    .enableHiveSupport() \
    .getOrCreate()

# Start local Ray inside Spark executor node
ray.init(num_gpus=2)
setup_ray_cluster()  # Optional in newer ray-spark integrations

# Define Ray task (simulate processing)
@ray.remote(num_gpus=1)
def process_partition(batch):
    # Your GPU-based logic here (e.g., SentenceTransformer, Torch, etc.)
    return batch

# JDBC config
jdbc_url = "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction?useSSL=false&serverTimezone=UTC&connectionTimeZone=Asia/Jakarta"
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read MySQL into Spark DataFrame
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=2,  # Match number of GPUs for simplicity
    properties=properties
)

# Collect partitions and send to Ray
batches = [row.asDict() for row in df.collect()]
futures = [process_partition.remote(pd.DataFrame([batch])) for batch in batches]
results = ray.get(futures)
final_df = spark.createDataFrame(pd.concat(results))

# Write to Hive
final_df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions_gpu")
spark.stop()
ray.shutdown()