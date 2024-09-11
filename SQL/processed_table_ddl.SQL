CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`processed_fraud_credit` (
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
)