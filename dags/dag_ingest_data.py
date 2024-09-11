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

input_create_table_query = """CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`fraud_credit` (
	`timestamp` bigint COMMENT 'The timestamp of the transaction, represented as a Unix epoch time in milliseconds.',
	`sending_address` string COMMENT 'The address from which the transaction was sent.',
	`receiving_address` string COMMENT 'The address to which the transaction was sent.',
	`amount` double COMMENT 'The amount of money involved in the transaction.',
	`transaction_type` string COMMENT 'The type of transaction (e.g., purchase, transfer).',
	`location_region` string COMMENT 'The geographical region where the transaction originated.',
	`ip_prefix` string COMMENT 'The IP address prefix associated with the transaction.',
	`login_frequency` int COMMENT 'The frequency of logins by the user associated with the transaction.',
	`session_duration` int COMMENT 'The duration of the users session in seconds.',
	`purchase_pattern` string COMMENT 'A pattern or category describing the users purchase behavior.',
	`age_group` string COMMENT 'The age group of the user involved in the transaction.',
	`risk_score` double COMMENT 'A score indicating the risk level of the transaction.',
	`anomaly` string COMMENT 'A flag or description indicating whether the transaction is considered anomalous.'
) COMMENT 'The fraud_credit table is designed to store transaction data related to credit fraud detection. Each row in the table represents a single transaction with various attributes that can be used for analysis and fraud detection.'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://airflow-localiza/data-input/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1' -- Pula a primeira linha (Header)
)"""

processed_create_table_query = """CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`processed_fraud_credit` (
	`timestamp` bigint COMMENT 'The timestamp of the transaction, represented as a Unix epoch time in milliseconds.',
	`sending_address` string COMMENT 'The address from which the transaction was sent.',
	`receiving_address` string COMMENT 'The address to which the transaction was sent.',
	`amount` double COMMENT 'The amount of money involved in the transaction.',
	`transaction_type` string COMMENT 'The type of transaction (e.g., purchase, transfer).',
	`location_region` string COMMENT 'The geographical region where the transaction originated.',
	`ip_prefix` string COMMENT 'The IP address prefix associated with the transaction.',
	`login_frequency` int COMMENT 'The frequency of logins by the user associated with the transaction.',
	`session_duration` int COMMENT 'The duration of the users session in seconds.',
	`purchase_pattern` string COMMENT 'A pattern or category describing the users purchase behavior.',
	`age_group` string COMMENT 'The age group of the user involved in the transaction.',
	`risk_score` double COMMENT 'A score indicating the risk level of the transaction.',
	`anomaly` string COMMENT 'A flag or description indicating whether the transaction is considered anomalous.'
) COMMENT 'The fraud_credit table is designed to store transaction data related to credit fraud detection. Each row in the table represents a single transaction with various attributes that can be used for analysis and fraud detection.'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://airflow-localiza/processed_data/'
TBLPROPERTIES (
	'classification' = 'csv',
	'skip.header.line.count' = '1' -- Pula a primeira linha (Header)
)"""

ingestion_query = """
INSERT INTO default.processed_fraud_credit
SELECT 
    timestamp AS timestamp,
    sending_address AS sending_address,
    receiving_address AS receiving_address,
    amount AS amount,
    transaction_type AS transaction_type,
    location_region AS location_region,
    ip_prefix AS ip_prefix,
    login_frequency AS login_frequency,
    session_duration AS session_duration,
    purchase_pattern AS purchase_pattern,
    age_group AS age_group,
    risk_score AS risk_score,
    anomaly AS anomaly
FROM default.fraud_credit"""

data_quality_create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`data_quality_table` (
	`source_table_row_count` bigint COMMENT 'Number of rows in fraud_credit',
	`destination_table_row` bigint COMMENT 'Number of rows in processed_fraud_credit',
	`integrity` double COMMENT 'Number of rows in fraud_credit/processed_fraud_credit',
	`rows_with_errors` bigint COMMENT 'Numbers of rows with vality problems AKA errors'
) COMMENT 'Data quality table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://airflow-localiza/data-quality/'
TBLPROPERTIES (
	'classification' = 'csv')
"""

data_quality_query = """
INSERT INTO default.data_quality_table
SELECT (
		SELECT COUNT(*) AS source_table_row_count
		FROM "default"."fraud_credit"
	) AS source_table_row_count,
	COUNT(*) AS destination_table_row,
	COUNT(*) / 
	CAST((
		SELECT COUNT(*) AS source_table_row_count
		FROM "default"."fraud_credit"
	) AS double) AS integrity,
	(SELECT COUNT(*) FROM "processed_fraud_credit" WHERE location_region NOT IN ('Europe', 'South America', 'Asia', 'Africa', 'North America')) AS rows_with_errors
	FROM "default"."processed_fraud_credit"
"""

ingestion_monitoring_dataset = Dataset("s3://airflow-localiza/datasets-airflow/ingestion_monitoring")
with DAG("ingest_data", start_date = datetime(2024,1,1),
         schedule_interval = "@once", catchup = False) as dag:
            sensor_file_s3 = S3KeySensor(
                    task_id = "sensor_file_s3",
                    aws_conn_id = "s3_athena",
                    bucket_key = "s3://airflow-localiza/data-input/df_fraud_credit.csv"
            )

            input_create_table = AWSAthenaOperator(
				task_id='input_create_table',
				query=input_create_table_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
            
            processed_create_table = AWSAthenaOperator(
				task_id='processed_create_table',
				query=processed_create_table_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
            data_quality_create_table = AWSAthenaOperator(
				task_id='data_quality_create_table',
				query=data_quality_create_table_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
            
            ingest_data = AWSAthenaOperator(
				task_id='ingest_data',
				query=ingestion_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza',
                outlets=[ingestion_monitoring_dataset]
			)
            
            calculate_data_quality_metrics = AWSAthenaOperator(
				task_id='calculate_data_quality_metrics',
				query=data_quality_query,
                database="default",
                aws_conn_id = "s3_athena",
                region_name="us-east-1",
				output_location='s3://ebetcel-athena-results/teste-localiza'
			)
            
            [input_create_table, processed_create_table, data_quality_create_table] >> sensor_file_s3 >> ingest_data >> calculate_data_quality_metrics