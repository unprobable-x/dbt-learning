WITH support_tickets AS (
  SELECT
    'TICKET' || LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 6, '0') AS ticket_id,
    'CUST' || LPAD(CAST(FLOOR(RAND() * 1000) + 1 AS STRING), 6, '0') AS customer_id,
    CASE 
      WHEN RAND() < 0.2 THEN 'open'
      WHEN RAND() < 0.3 THEN 'on-hold'
      ELSE 'closed'
    END AS status,
    TIMESTAMP_ADD(
      TIMESTAMP '2020-01-01 00:00:00',
      INTERVAL FLOOR(RAND() * 1000000) MINUTE
    ) AS created_date,
    CASE 
      WHEN status = 'closed' THEN 
        TIMESTAMP_ADD(
          created_date,
          INTERVAL FLOOR(RAND() * 10080) + 60 MINUTE  -- 1 hour to 7 days
        )
      ELSE NULL
    END AS closed_date,
    CASE 
      WHEN RAND() < 0.3 THEN 'bug'
      WHEN RAND() < 0.5 THEN 'incident'
      WHEN RAND() < 0.8 THEN 'question'
      ELSE 'feature request'
    END AS ticket_category
  FROM UNNEST(GENERATE_ARRAY(1, 3000)) AS t
)
SELECT * FROM support_tickets
WHERE (status != 'closed' OR closed_date > created_date)
ORDER BY created_date; 