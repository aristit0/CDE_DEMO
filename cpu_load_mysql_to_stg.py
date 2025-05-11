from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Load MySQL to Hive Staging").enableHiveSupport().getOrCreate()

jdbc_url = "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction?useSSL=false&serverTimezone=UTC&connectionTimeZone=Asia/Jakarta"
properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="mobile_transactions",
    column="transaction_id",            # numeric and indexed column
    lowerBound=1,
    upperBound=10000000,
    numPartitions=10,                   # can be tuned, try 10–20
    properties=properties
)

#df = spark.read.jdbc(url=jdbc_url, table="mobile_transactions", properties=properties)

df.write.mode("overwrite").format("parquet").saveAsTable("stg.mobile_transactions")

spark.stop()