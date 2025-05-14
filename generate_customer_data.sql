WITH customer_data AS (
  SELECT
    'CUST' || LPAD(CAST(ROW_NUMBER() OVER () AS STRING), 6, '0') AS customer_id,
    DATE_ADD('2020-01-01', INTERVAL FLOOR(RAND() * 1000) DAY) AS acquisition_date,
    CASE 
      WHEN RAND() < 0.25 THEN 'east'
      WHEN RAND() < 0.5 THEN 'west'
      WHEN RAND() < 0.75 THEN 'north'
      ELSE 'south'
    END AS region,
    CASE 
      WHEN RAND() < 0.33 THEN 'technology'
      WHEN RAND() < 0.66 THEN 'banking'
      ELSE 'retail'
    END AS industry
  FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS t
)
SELECT * FROM customer_data
ORDER BY acquisition_date; 