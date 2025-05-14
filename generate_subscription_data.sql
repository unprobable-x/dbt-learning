WITH subscription_data AS (
  SELECT
    'CUST' || LPAD(CAST(FLOOR(RAND() * 1000) + 1 AS STRING), 6, '0') AS customer_id,
    'PROD' || LPAD(CAST(FLOOR(RAND() * 50) + 1 AS STRING), 4, '0') AS product_id,
    FLOOR(RAND() * 5) + 1 AS quantity,
    DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1000) DAY) AS start_date,
    DATE_ADD(
      DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1000) DAY),
      INTERVAL FLOOR(RAND() * 730) + 30 DAY
    ) AS end_date,
    CASE 
      WHEN FLOOR(RAND() * 3) = 0 THEN 999
      WHEN FLOOR(RAND() * 3) = 1 THEN 1999
      ELSE 4999
    END AS arr
  FROM UNNEST(GENERATE_ARRAY(1, 2000)) AS t
)
SELECT * FROM subscription_data
WHERE end_date > start_date
ORDER BY start_date; 