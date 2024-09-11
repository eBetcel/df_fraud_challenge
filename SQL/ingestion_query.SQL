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
FROM "default"."fraud_credit" 