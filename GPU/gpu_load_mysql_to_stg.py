import ray
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

# Initialize Ray with GPU support
ray.init(num_gpus=2)

# Define Ray task to fetch a batch from MySQL
@ray.remote(num_gpus=1)
def fetch_batch(offset, limit):
    from sqlalchemy import create_engine, text

    # Recreate engine inside Ray worker
    engine = create_engine("mysql+pymysql://cloudera:Admin123@cdpm1.cloudeka.ai/transaction")

    # Use connection explicitly to avoid schema conflict
    with engine.connect() as conn:
        query = text(f"""
            SELECT transaction_id, account_id, transaction_type,
                   amount, currency, status, transaction_timestamp
            FROM mobile_transactions
            LIMIT {limit} OFFSET {offset}
        """)
        df = pd.read_sql(query, conn)

    return df

# Define batch settings
batch_size = 500_000
num_batches = 4  # Adjust based on total rows and GPU

# Launch Ray tasks in parallel
futures = [fetch_batch.remote(i * batch_size, batch_size) for i in range(num_batches)]
results = ray.get(futures)

# Combine all batches into one DataFrame
combined_df = pd.concat(results, ignore_index=True)

# Start Spark session
spark = SparkSession.builder \
    .appName("Ingest MySQL with Ray") \
    .enableHiveSupport() \
    .getOrCreate()

# Convert Pandas to Spark DataFrame
spark_df = spark.createDataFrame(combined_df)

# Optional: repartition for write optimization
spark_df = spark_df.repartition(8)

# Write to Hive staging table
spark_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("stg.mobile_transactions_gpu")

spark.stop()
ray.shutdown()