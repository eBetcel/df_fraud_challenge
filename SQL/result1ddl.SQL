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