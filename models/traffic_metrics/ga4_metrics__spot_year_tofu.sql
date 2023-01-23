WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu') }}
),

latest AS (
    SELECT MAX(date) AS latest_date
    FROM src
)

SELECT CURRENT_DATE() AS dashboard_date,
        *
FROM src
WHERE date = (SELECT latest_date FROM latest)

UNION ALL

SELECT DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS dashboard_date,
        *
FROM src
WHERE date = (SELECT DATE_TRUNC(latest_date, YEAR) FROM latest)
