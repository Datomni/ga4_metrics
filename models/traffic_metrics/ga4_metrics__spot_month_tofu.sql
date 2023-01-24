WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu')}}
),

latest AS (
    SELECT MAX(date) AS latest_date
    FROM src
)

SELECT current_date() as dashboard_date, 
       *
FROM src
WHERE date = (SELECT latest_date FROM latest)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL -1 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = (SELECT DATE_ADD(latest_date,  INTERVAL -30 DAY) FROM latest)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL -2 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = (SELECT DATE_ADD(latest_date,  INTERVAL - 90 DAY) FROM latest)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL - 3 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = (SELECT DATE_ADD(latest_date,  INTERVAL - 180 DAY) FROM latest)
