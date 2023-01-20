WITH src AS (
    SELECT *
    FROM {{ ref('aggregate_day_tofu')}}
)

SELECT current_date() AS dashboard_date,
       CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY),' - ',CURRENT_DATE()) AS period, 
       COALESCE(SUM(uv_organic_traffic_unique), 0) AS total_uv_organic_traffic,
       COALESCE(SUM(uv_paid_traffic_unique), 0) AS total_uv_paid_traffic,
       COALESCE(SUM(uv_social_traffic_unique), 0) AS total_uv_social_traffic
FROM src
WHERE date >= (current_date() - INTERVAL 30 DAY)

UNION ALL

SELECT date_add(current_date(), INTERVAL -1 DAY) AS dashboard_date,
       CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -60 DAY),' - ',DATE_ADD(CURRENT_DATE(), INTERVAL -31 DAY)) AS period, 
       COALESCE(SUM(uv_organic_traffic_unique), 0) AS total_uv_organic_traffic,
       COALESCE(SUM(uv_paid_traffic_unique), 0) AS total_uv_paid_traffic,
       COALESCE(SUM(uv_social_traffic_unique), 0) AS total_uv_social_traffic
FROM src
WHERE date <= (current_date() - INTERVAL 31 DAY) AND 
      date >=  (current_date() - INTERVAL 60 DAY)
