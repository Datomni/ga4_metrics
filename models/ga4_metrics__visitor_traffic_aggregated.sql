WITH src AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views') }}
),

last30 AS (
    SELECT COUNT(DISTINCT user_pseudo_id) AS last_30_days_unique_visitors, 
            utm_medium
    FROM src
    WHERE CAST(event_timestamp_utc AS DATE) >= (CURRENT_DATE() - 30)
    GROUP BY utm_medium
),

last90 AS (
    SELECT COUNT(DISTINCT user_pseudo_id) AS last_90_days_unique_visitors, 
            utm_medium
    FROM src 
    WHERE CAST(event_timestamp_utc AS DATE) <= (CURRENT_DATE() - 31) AND
            CAST(event_timestamp_utc AS DATE) >= (CURRENT_DATE() - 90)
    GROUP BY utm_medium
),

last180 AS (
    SELECT COUNT(DISTINCT user_pseudo_id) AS last_180_days_unique_visitors, 
            utm_medium
    FROM src
    WHERE CAST(event_timestamp_utc AS DATE) <= (CURRENT_DATE() - 91) AND
            CAST(event_timestamp_utc AS DATE) >= (CURRENT_DATE() - 180)
    GROUP BY utm_medium
)

SELECT LOWER(utm_medium) AS utm_medium,
       COALESCE(last_30_days_unique_visitors, 0) AS last_30_days_unique_visitors,
       COALESCE(last_90_days_unique_visitors, 0) AS last_90_days_unique_visitors,
       COALESCE(last_180_days_unique_visitors, 0) AS last_180_days_unique_visitors
FROM last30
FULL OUTER JOIN last90 USING (utm_medium)
FULL OUTER JOIN last180 USING (utm_medium)
