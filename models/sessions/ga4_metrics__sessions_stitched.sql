WITH pageviews_sessionized AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views_sessionized') }}
),

referrer_mapping AS (
    SELECT * 
    FROM {{ ref('referrer_mapping') }}
),

agg AS (
    SELECT DISTINCT session_id,
            user_pseudo_id,
            anonymous_id,
            MIN(event_timestamp_utc) OVER ( PARTITION BY session_id ) AS session_start_tstamp,
            MAX(event_timestamp_utc) OVER ( PARTITION BY session_id ) AS session_end_tstamp,
            COUNT(DISTINCT event_timestamp_utc) OVER ( PARTITION BY session_id ) AS page_views,
            FIRST_VALUE(utm_source IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_source,
            
            FIRST_VALUE(utm_medium IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_medium,
            FIRST_VALUE(utm_campaign IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_campaign,
            FIRST_VALUE(utm_term IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_term,
            FIRST_VALUE(gclid IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gclid,
            FIRST_VALUE(referrer IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS referrer,
            FIRST_VALUE(referrer_host IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS referrer_host,
            FIRST_VALUE(device IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device,
            FIRST_VALUE(device_category IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_category,
            FIRST_VALUE(page_url IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_url,
            FIRST_VALUE(page_url_host IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_url_host,
            
            LAST_VALUE(page_url IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_url,
            LAST_VALUE(page_url_host IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_url_host
    FROM pageviews_sessionized
),

diffs AS (
    SELECT  *,
            DATETIME_DIFF(CAST(session_end_tstamp AS DATETIME), CAST(session_start_tstamp AS DATETIME), second) AS duration_in_s
    FROM agg
),

tiers AS (
    SELECT  *,
            CASE
                WHEN duration_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
                WHEN duration_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
                WHEN duration_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
                WHEN duration_in_s > 59 THEN '60s or more'
                ELSE NULL END AS duration_in_s_tier
    FROM diffs
),

mapped AS (
    SELECT  tiers.*,
            referrer_mapping.medium as referrer_medium,
            referrer_mapping.source as referrer_source
    FROM tiers
    LEFT JOIN referrer_mapping ON tiers.referrer_host = referrer_mapping.host
)

SELECT * FROM mapped
