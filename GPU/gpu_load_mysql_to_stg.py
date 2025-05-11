from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("Load MySQL to Hive Staging with Ray Partition") \
    .enableHiveSupport() \
    .getOrCreate()

# JDBC Configuration
jdbc_url = (
    "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction"
    "?useSSL=false"
    "&serverTimezone=UTC"
    "&connectionTimeZone=Asia/Jakarta"
    "&autoReconnect=true"
    "&socketTimeout=600000"
    "&connectTimeout=30000"
    "&interactiveClient=true"
    "&tcpKeepAlive=true"
)

properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read from MySQL using partitioned parallel read
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=2,  # Adjust according to GPU count
    properties=properties
)

# Define the partition-level GPU logic using Ray
def ray_partition_processor(iterator):
    import ray
    import pandas as pd

    data = list(iterator)
    if not data:
        return iter([])

    df_batch = pd.DataFrame(data)

    # Initialize Ray after JDBC read
    ray.init(num_gpus=1, ignore_reinit_error=True)

    @ray.remote(num_gpus=1)
    def gpu_task(batch_df):
        # Simulate GPU-based processing (e.g., embeddings, NLP, etc.)
        return batch_df

    future = gpu_task.remote(df_batch)
    result_df = ray.get(future)

    ray.shutdown()

    # Return each row back to Spark
    for row in result_df.itertuples(index=False, name=None):
        yield row

# Apply the GPU batch logic to each Spark partition
processed_rdd = df.rdd.mapPartitions(ray_partition_processor)

# Recreate DataFrame with original schema
final_df = spark.createDataFrame(processed_rdd, schema=df.schema)

# Write to Hive staging table
final_df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions_gpu")

spark.stop()