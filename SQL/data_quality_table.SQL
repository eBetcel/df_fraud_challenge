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
	