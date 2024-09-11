CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`data_quality_table` (
	`location_region` string COMMENT 'Location region',
	`avg_risk_score` double COMMENT 'Avarage risk score per location region',
) COMMENT 'Result 1 table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://airflow-localiza/results/1/'
TBLPROPERTIES (
	'classification' = 'csv')