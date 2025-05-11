


#========= CPU
CREATE EXTERNAL TABLE IF NOT EXISTS stg.mobile_transactions (
    transaction_id BIGINT,
    account_id STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    transaction_timestamp TIMESTAMP,
    merchant STRING,
    location STRING
)
STORED AS PARQUET
LOCATION 'hdfs://cloudeka-hdfs/warehouse/tablespace/external/hive/stg.db/mobile_transactions'
TBLPROPERTIES (
  'external.table.purge' = 'true'
);


CREATE EXTERNAL TABLE IF NOT EXISTS datamart.mobile_tx_summary (
    year INT,
    month INT,
    transaction_type STRING,
    currency STRING,
    total_amount DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://cloudeka-hdfs/warehouse/tablespace/external/hive/datamart.db/mobile_tx_summary'
TBLPROPERTIES (
  'external.table.purge' = 'true'
);


#========= GPU 
CREATE EXTERNAL TABLE IF NOT EXISTS stg.mobile_transactions_gpu (
    transaction_id BIGINT,
    account_id STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    transaction_timestamp TIMESTAMP,
    merchant STRING,
    location STRING
)
STORED AS PARQUET
LOCATION 'hdfs://cloudeka-hdfs/warehouse/tablespace/external/hive/stg.db/mobile_transactions_gpu'
TBLPROPERTIES (
  'external.table.purge' = 'true'
);


CREATE EXTERNAL TABLE IF NOT EXISTS datamart.mobile_tx_summary_gpu (
    year INT,
    month INT,
    transaction_type STRING,
    currency STRING,
    total_amount DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://cloudeka-hdfs/warehouse/tablespace/external/hive/datamart.db/mobile_tx_summary_gpu'
TBLPROPERTIES (
  'external.table.purge' = 'true'
);
