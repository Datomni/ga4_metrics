WITH src AS (
    SELECT *
    FROM {{ ref('aggregate_day_tofu') }}
)

SELECT CASE WHEN date = DATE_TRUNC(CURRENT_DATE(), YEAR) THEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
            ELSE CURRENT_DATE() END AS dashboard_date,
       date, 
       uv_organic_traffic_unique,
       uv_paid_traffic_unique,
       uv_social_traffic_unique
FROM src
WHERE date = (SELECT MAX(date) FROM src) OR 
      date = (SELECT DATE_TRUNC(CURRENT_DATE(), YEAR))
