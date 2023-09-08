WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_conversions')}}
),

latest AS (
    SELECT MAX(date) AS latest_date
    FROM src
)

SELECT {{ dbt_date.today() }} as dashboard_date,
       *
FROM src
WHERE date = (SELECT latest_date FROM latest)

UNION ALL

SELECT {{ dbt_date.n_days_ago(1) }} as dashboard_date,
       *
FROM src
WHERE date = (SELECT {{ dbt_date.n_days_ago(30, date="latest_date") }} FROM latest)

UNION ALL

SELECT {{ dbt_date.n_days_ago(2) }} as dashboard_date,
       *
FROM src
WHERE date = (SELECT {{ dbt_date.n_days_ago(90, date="latest_date") }} FROM latest)

UNION ALL

SELECT {{ dbt_date.n_days_ago(3) }} as dashboard_date,
       *
FROM src
WHERE date = (SELECT {{ dbt_date.n_days_ago(180, date="latest_date") }} FROM latest)
