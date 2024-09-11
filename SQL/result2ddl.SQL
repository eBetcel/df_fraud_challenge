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