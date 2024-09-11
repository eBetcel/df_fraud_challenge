from airflow import DAG
"""
This DAG is responsible for ingesting data into two external tables in Athena: `fraud_credit2` and `processed_fraud_credit`. It performs the following tasks:
1. `sensor_file_s3`: A sensor task that waits for a file (`df_test.csv`) to be available in the S3 bucket `s3://airflow-localiza/data-input/`. It uses the `S3KeySensor` from the `airflow.providers.amazon.aws.sensors.s3` module.
2. `input_create_table`: An Athena operator task that creates the `fraud_credit` table if it doesn't already exist. It executes the `input_create_table_query` SQL statement and stores the result in the `default` database. It uses the `AWSAthenaOperator` from the `airflow.contrib.operators.aws_athena_operator` module.
3. `processed_create_table`: An Athena operator task that creates the `processed_fraud_credit` table if it doesn't already exist. It executes the `processed_create_table_query` SQL statement and stores the result in the `default` database. It uses the `AWSAthenaOperator` from the `airflow.contrib.operators.aws_athena_operator` module.
4. `ingest_data`: An Athena operator task that performs the data ingestion into the `fraud_credit2` table. It executes the `ingestion_query` SQL statement and stores the result in the `default` database. It uses the `AWSAthenaOperator` from the `airflow.contrib.operators.aws_athena_operator` module.
The DAG is scheduled to run once and has the `catchup` parameter set to `False`, meaning it won't backfill any missed runs.
"""
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import Dataset
# from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
# from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

result1_create_table_query = """
CREATE EXTERNAL TABLE `results1`(
  `location_region` string COMMENT 'Location region', 
  `avg_risk_score` double COMMENT 'Avarage risk score per location region')
COMMENT 'Result 1 table'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://airflow-localiza/results/1'
TBLPROPERTIES (
  'classification'='csv', 
  'transient_lastDdlTime'='1726046997')
"""

result2_create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`results2` (
	`receiving_address` string COMMENT 'receiving_address',
	`amount` double COMMENT 'amount',
	timestamp bigint COMMENT 'timestamp'
) COMMENT 'Result 1 table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://airflow-localiza/results/2/'
TBLPROPERTIES (
	'classification' = 'csv')
"""

result1_calculate_result_query = """
INSERT INTO default.results1
SELECT location_region, AVG(risk_score) AS avg_risk_score
FROM processed_fraud_credit
GROUP BY location_region
ORDER BY avg_risk_score DESC
"""
result2_calculate_result_query = """
INSERT INTO default.results2
WITH cte_recent_sales AS (
    SELECT receiving_address, amount, timestamp,
           ROW_NUMBER() OVER (PARTITION BY receiving_address ORDER BY timestamp DESC) AS rn
    FROM processed_fraud_credit
    WHERE transaction_type = 'sale'
)
SELECT receiving_address, amount, timestamp
FROM cte_recent_sales
WHERE rn = 1
ORDER BY amount DESC
LIMIT 3
"""


ingestion_monitoring_dataset = Dataset("s3://airflow-localiza/datasets-airflow/ingestion_monitoring")
with DAG("calculate_results", start_date = datetime(2024,1,1),
         schedule = [ingestion_monitoring_dataset], catchup = False) as dag:
            result1_create_table = AWSAthenaOperator(
				task_id='result1_create_table',
				query=result1_create_table_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
            
            result2_create_table = AWSAthenaOperator(
				task_id='result2_create_table',
				query=result2_create_table_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
        
            result1_calculate_result = AWSAthenaOperator(
				task_id='result1_calculate_result',
				query=result1_calculate_result_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)

            result2_calculate_result = AWSAthenaOperator(
				task_id='result2_calculate_result',
				query=result2_calculate_result_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)

            # Setando as dependências das tasks
            result1_create_table >> result1_calculate_result
            result2_create_table >> result2_calculate_result