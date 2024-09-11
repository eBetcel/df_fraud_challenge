SELECT location_region, AVG(risk_score) AS avg_risk_score
FROM processed_fraud_credit
GROUP BY location_region
ORDER BY avg_risk_score DESC