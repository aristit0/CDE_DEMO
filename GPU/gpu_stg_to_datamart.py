from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum
import ray
import pandas as pd

spark = SparkSession.builder \
    .appName("Transform to Datamart with Ray") \
    .enableHiveSupport() \
    .getOrCreate()

ray.init(num_gpus=2)

@ray.remote(num_gpus=1)
def aggregate_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch['year'] = pd.to_datetime(batch['transaction_timestamp']).dt.year
    batch['month'] = pd.to_datetime(batch['transaction_timestamp']).dt.month
    return batch.groupby(['year', 'month', 'transaction_type', 'currency'])['amount'].sum().reset_index(name='total_amount')

# Read from Hive
df = spark.sql("SELECT * FROM stg.mobile_transactions_gpu")

# Collect and batch in Pandas for Ray processing
batches = [row.asDict() for row in df.collect()]
futures = [aggregate_batch.remote(pd.DataFrame([batch])) for batch in batches]
results = ray.get(futures)
final_df = spark.createDataFrame(pd.concat(results))

# Save to datamart
final_df.write.mode("overwrite").format("parquet").saveAsTable("datamart.mobile_tx_summary_gpu")
spark.stop()
ray.shutdown()