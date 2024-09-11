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