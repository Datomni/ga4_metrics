WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_conversions') }}
),

latest AS (
    SELECT MAX(date) AS latest_date
    FROM src
)

SELECT {{ dbt_date.today() }} AS dashboard_date,
        *
FROM src
WHERE date = (SELECT latest_date FROM latest)

UNION ALL

SELECT {{ dbt_date.n_days_ago(1) }} AS dashboard_date,
        *
FROM src
WHERE date = (SELECT DATE({{ dbt.date_trunc("year", "latest_date") }}) FROM latest)
