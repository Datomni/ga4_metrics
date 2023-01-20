WITH src AS (
    SELECT *
    FROM {{ ref('aggregate_day_tofu')}}
)

-- use max date from data instead
SELECT current_date() as dashboard_date, 
       *
FROM src
WHERE date = DATE_ADD(current_date(), INTERVAL -1 DAY)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL -1 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = DATE_ADD(current_date(), INTERVAL -30 DAY)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL -2 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = DATE_ADD(current_date(), INTERVAL -90 DAY)

UNION ALL

SELECT DATE_ADD(current_date(), INTERVAL -3 DAY) as dashboard_date, 
       * 
FROM src
WHERE date = DATE_ADD(current_date(), INTERVAL -180 DAY)
