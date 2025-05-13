from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ingest MySQL to Hive (JDBC)") \
    .enableHiveSupport() \
    .getOrCreate()

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

df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",
    lowerBound=1,
    upperBound=1000000000,
    numPartitions=2,
    properties=properties
)

df = df.repartition(8)

df.write.mode("overwrite") \
    .format("parquet") \
    .saveAsTable("stg.mobile_transactions_gpu")

spark.stop()