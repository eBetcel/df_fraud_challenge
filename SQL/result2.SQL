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
LIMIT 3;