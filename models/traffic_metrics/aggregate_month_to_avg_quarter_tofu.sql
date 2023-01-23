WITH src AS (
    SELECT *
    FROM {{ ref('aggregate_day_tofu')}}
),

traffic_last30_days AS (
    SELECT CURRENT_DATE() AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique) AS total_uv_organic_traffic,
           SUM(uv_paid_traffic_unique) AS total_uv_paid_traffic,
           SUM(uv_social_traffic_unique) AS total_uv_social_traffic
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
),

traffic_last90_days AS (
    SELECT date_add(CURRENT_DATE(), INTERVAL -1 DAY) AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique) AS total_uv_organic_traffic,
           SUM(uv_paid_traffic_unique) AS total_uv_paid_traffic,
           SUM(uv_social_traffic_unique) AS total_uv_social_traffic
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY)
),

traffic_last180_days AS (
    SELECT date_add(CURRENT_DATE(), INTERVAL -2 DAY) AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -180 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique) AS total_uv_organic_traffic,
           SUM(uv_paid_traffic_unique) AS total_uv_paid_traffic,
           SUM(uv_social_traffic_unique) AS total_uv_social_traffic
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -180 DAY)
)

SELECT dashboard_date,
       period,
       total_uv_organic_traffic,
       total_uv_paid_traffic,
       total_uv_social_traffic
FROM traffic_last30_days

UNION ALL

SELECT dashboard_date,
       period,
       total_uv_organic_traffic,
       total_uv_paid_traffic,
       total_uv_social_traffic
FROM traffic_last90_days

UNION ALL

SELECT dashboard_date,
       period,
       total_uv_organic_traffic,
       total_uv_paid_traffic,
       total_uv_social_traffic
FROM traffic_last180_days
