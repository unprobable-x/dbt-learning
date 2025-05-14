WITH login_events AS (
  SELECT
    'USER' || LPAD(CAST(FLOOR(RAND() * 2000) + 1 AS STRING), 6, '0') AS user_id,
    'CUST' || LPAD(CAST(FLOOR(RAND() * 1000) + 1 AS STRING), 6, '0') AS customer_id,
    CASE 
      WHEN RAND() < 0.1 THEN 'admin'
      WHEN RAND() < 0.4 THEN 'advanced'
      ELSE 'standard'
    END AS user_type,
    TIMESTAMP_ADD(
      TIMESTAMP '2020-01-01 00:00:00',
      INTERVAL FLOOR(RAND() * 1000000) MINUTE
    ) AS login_timestamp
  FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS t
)
SELECT * FROM login_events
ORDER BY login_timestamp; 