WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__aggregate_day_tofu')}}
),

unioned AS (
    SELECT current_date() AS dashboard_date,
        CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -365 DAY),' - ',CURRENT_DATE()) AS period, 
        SUM(uv_organic_traffic_unique) AS total_uv_organic_traffic,
        SUM(uv_paid_traffic_unique) AS total_uv_paid_traffic,
        SUM(uv_social_traffic_unique) AS total_uv_social_traffic,
        SUM(uv_earned_media_traffic_unique) AS total_uv_earned_media_traffic_unique,
        SUM(uv_marketplace_traffic_unique) AS total_uv_marketplace_traffic_unique,
        SUM(uv_referral_traffic_unique) AS total_uv_referral_traffic_unique
    FROM src
    WHERE date >= DATE_ADD(CURRENT_DATE(), INTERVAL -365 DAY)

    UNION ALL

    SELECT date_add(current_date(), INTERVAL -1 DAY) AS dashboard_date,
        CONCAT(DATE_ADD(CURRENT_DATE(), INTERVAL -730 DAY),' - ',DATE_ADD(CURRENT_DATE(), INTERVAL -366 DAY)) AS period,
        SUM(uv_organic_traffic_unique) AS total_uv_organic_traffic,
        SUM(uv_paid_traffic_unique) AS total_uv_paid_traffic,
        SUM(uv_social_traffic_unique) AS total_uv_social_traffic,
        SUM(uv_earned_media_traffic_unique) AS total_uv_earned_media_traffic_unique,
        SUM(uv_marketplace_traffic_unique) AS total_uv_marketplace_traffic_unique,
        SUM(uv_referral_traffic_unique) AS total_uv_referral_traffic_unique
    FROM src
    WHERE date <= DATE_ADD(CURRENT_DATE(), INTERVAL -366 DAY) AND
        date >= DATE_ADD(CURRENT_DATE(), INTERVAL -730 DAY)
)

SELECT dashboard_date,
        period,
        COALESCE(uv_organic_traffic_unique, 0) AS total_uv_organic_traffic,
        COALESCE(uv_paid_traffic_unique, 0) AS total_uv_paid_traffic,
        COALESCE(uv_social_traffic_unique, 0) AS total_uv_social_traffic,
        COALESCE(uv_earned_media_traffic_unique, 0) AS total_uv_earned_media_traffic_unique,
        COALESCE(uv_marketplace_traffic_unique, 0) AS total_uv_marketplace_traffic_unique,
        COALESCE(uv_referral_traffic_unique, 0) AS total_uv_referral_traffic_unique
FROM unioned
