WITH pageviews AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views') }}
),

numbered AS (
    --This CTE is responsible for assigning an all-time page view number for a
    --given user_pseudo_id.
    SELECT  *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp_utc) AS page_view_number
    FROM pageviews
),


session_ids AS (
    SELECT  user_pseudo_id,
            user_id,
            anonymous_id,
            event_timestamp_utc,
            session_id,
            session_number,
            page_title,
            page_url,
            page_url_host,
            referrer,
            referrer_host,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_term,
            gclid,
            device,
            device_category,
            page_view_number,
    FROM numbered
)

SELECT * 
FROM session_ids
