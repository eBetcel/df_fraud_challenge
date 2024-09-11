CREATE EXTERNAL TABLE IF NOT EXISTS fraud_credit(
  timestamp TIMESTAMP, 
  sending_address STRING,
  receiving_address STRING,
  amount DOUBLE,
  transaction_type STRING,
  location_region STRING,
  ip_prefix DOUBLE,
  login_frequency INT,
  session_duration INT,
  purchase_pattern STRING,
  age_group STRING,
  risk_score STRING,
  anomaly STRING
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://airflow-localiza/data-input/'