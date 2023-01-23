WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu')}}
),

traffic_last30_days AS (
    SELECT CURRENT_DATE() AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique) AS monthly_avg_uv_organic_traffic,
           SUM(uv_paid_traffic_unique) AS monthly_avg_uv_paid_traffic,
           SUM(uv_social_traffic_unique) AS monthly_avg_uv_social_traffic,
           SUM(uv_earned_media_traffic_unique) AS monthly_avg_uv_earned_media_traffic_unique,
           SUM(uv_marketplace_traffic_unique) AS monthly_avg_uv_marketplace_traffic_unique,
           SUM(uv_referral_traffic_unique) AS monthly_avg_uv_referral_traffic_unique
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
),

traffic_last90_days AS (
    SELECT date_add(CURRENT_DATE(), INTERVAL -1 DAY) AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique)/3 AS monthly_avg_uv_organic_traffic,
           SUM(uv_paid_traffic_unique)/3 AS monthly_avg_uv_paid_traffic,
           SUM(uv_social_traffic_unique)/3 AS monthly_avg_uv_social_traffic,
           SUM(uv_earned_media_traffic_unique) AS monthly_avg_uv_earned_media_traffic_unique,
           SUM(uv_marketplace_traffic_unique) AS monthly_avg_uv_marketplace_traffic_unique,
           SUM(uv_referral_traffic_unique) AS monthly_avg_uv_referral_traffic_unique
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY)
),

traffic_last180_days AS (
    SELECT date_add(CURRENT_DATE(), INTERVAL -2 DAY) AS dashboard_date,
           CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -180 DAY),' - ',CURRENT_DATE()) AS period, 
           SUM(uv_organic_traffic_unique)/6 AS monthly_avg_uv_organic_traffic,
           SUM(uv_paid_traffic_unique)/6 AS monthly_avg_uv_paid_traffic,
           SUM(uv_social_traffic_unique)/6 AS monthly_avg_uv_social_traffic,
           SUM(uv_earned_media_traffic_unique) AS monthly_avg_uv_earned_media_traffic_unique,
           SUM(uv_marketplace_traffic_unique) AS monthly_avg_uv_marketplace_traffic_unique,
           SUM(uv_referral_traffic_unique) AS monthly_avg_uv_referral_traffic_unique
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -180 DAY)
),

unioned AS (
    SELECT *
    FROM traffic_last30_days

    UNION ALL

    SELECT *
    FROM traffic_last90_days

    UNION ALL

    SELECT *
    FROM traffic_last180_days
)

SELECT dashboard_date,
        period,
        COALESCE(monthly_avg_uv_organic_traffic, 0) AS monthly_avg_uv_organic_traffic,
        COALESCE(monthly_avg_uv_paid_traffic, 0) AS monthly_avg_uv_paid_traffic,
        COALESCE(monthly_avg_uv_social_traffic, 0) AS monthly_avg_uv_social_traffic,
        COALESCE(monthly_avg_uv_earned_media_traffic_unique, 0) AS monthly_avg_uv_earned_media_traffic_unique,
        COALESCE(uv_marketplace_traffic_unique, 0) AS monthly_avg_uv_marketplace_traffic_unique,
        COALESCE(uv_referral_traffic_unique, 0) AS monthly_avg_uv_referral_traffic_unique
FROM unioned
