from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("Load MySQL to Hive Staging with Ray Partition") \
    .enableHiveSupport() \
    .getOrCreate()

# JDBC connection
jdbc_url = (
    "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction"
    "?useSSL=false"
    "&serverTimezone=UTC"
    "&connectionTimeZone=Asia/Jakarta"
    "&socketTimeout=600000"
    "&connectTimeout=30000"
    "&autoReconnect=true"
)
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load MySQL to Spark DataFrame (partition = number of GPUs)
df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=2,
    properties=properties
)

# GPU processing inside each Spark partition using Ray
def ray_partition_processor(iterator):
    import ray
    import pandas as pd

    ray.init(num_gpus=1, ignore_reinit_error=True)

    @ray.remote(num_gpus=1)
    def gpu_task(batch):
        # Replace this with your actual GPU logic, like model.encode(batch)
        return batch

    data = list(iterator)
    if not data:
        return iter([])

    df_batch = pd.DataFrame(data)
    future = gpu_task.remote(df_batch)
    result_df = ray.get(future)
    ray.shutdown()

    for row in result_df.itertuples(index=False, name=None):
        yield row

# Apply processing to each partition
processed_rdd = df.rdd.mapPartitions(ray_partition_processor)
final_df = spark.createDataFrame(processed_rdd, schema=df.schema)

# Save to Hive
final_df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions_gpu")

spark.stop()