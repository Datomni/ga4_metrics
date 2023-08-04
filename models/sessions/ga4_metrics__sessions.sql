--TODO: make it incremental
WITH pageviews_sessionized AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views') }}
),

source_categories AS (
    SELECT * 
    FROM {{ ref('ga4_source_categories') }}
),

agg AS (
    SELECT DISTINCT
            -- session fields
            session_id,
            session_number,
            MIN(event_timestamp_utc) OVER ( PARTITION BY user_pseudo_id, session_id ) AS session_start_tstamp,
            MAX(event_timestamp_utc) OVER ( PARTITION BY user_pseudo_id, session_id ) AS session_end_tstamp,

            -- engagement fields
            -- TODO: is there a better way to get number of page views?
            COUNT(DISTINCT event_timestamp_utc) OVER ( PARTITION BY user_pseudo_id, session_id ) AS page_views,
            -- engaged_time_in_s

            -- user fields
            anonymous_id,
            user_id,
            user_pseudo_id,

            -- first page fields
            FIRST_VALUE(page_title IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_title,
            FIRST_VALUE(page_url IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_url,
            FIRST_VALUE(page_url_host IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_url_host,

            -- last page fields
            LAST_VALUE(page_title IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_title,
            LAST_VALUE(page_url IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_url,
            LAST_VALUE(page_url_host IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_url_host,

            -- referrer fields
            FIRST_VALUE(referrer IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS referrer,
            FIRST_VALUE(referrer_host IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS referrer_host,

            -- marketing fields
            FIRST_VALUE(utm_source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_source,
            FIRST_VALUE(utm_medium IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_medium,
            FIRST_VALUE(utm_campaign IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_campaign,
            FIRST_VALUE(utm_term IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS utm_term,
            FIRST_VALUE(gclid IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gclid,

            FIRST_VALUE(traffic_source_name IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_source_name,
            FIRST_VALUE(traffic_source_medium IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_source_medium,
            FIRST_VALUE(traffic_source_source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_source_source,
            FIRST_VALUE(platform IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS platform,

            -- geo fields
            FIRST_VALUE(geo_continent IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_continent,
            FIRST_VALUE(geo_country IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_country,
            FIRST_VALUE(geo_region IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_region,
            FIRST_VALUE(geo_city IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_city,
            FIRST_VALUE(geo_subcontinent IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_subcontinent,
            FIRST_VALUE(geo_metro IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS geo_metro,

            -- device fields
            FIRST_VALUE(device_category IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_category,
            FIRST_VALUE(device_mobile_brand_name IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_mobile_brand_name,
            FIRST_VALUE(device_mobile_model_name IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_mobile_model_name,
            FIRST_VALUE(device_operating_system IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_operating_system,
            FIRST_VALUE(device_operating_system_version IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_operating_system_version,
            FIRST_VALUE(device_vendor_id IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_vendor_id,
            FIRST_VALUE(device_advertising_id IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_advertising_id,
            FIRST_VALUE(device_language IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_language,
            FIRST_VALUE(device_browser IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_browser,
            FIRST_VALUE(device_browser_version IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_browser_version,
            FIRST_VALUE(device_web_info_browser IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_web_info_browser,
            FIRST_VALUE(device_web_info_browser_version IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_web_info_browser_version,
            FIRST_VALUE(device_web_info_hostname IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device_web_info_hostname,

            -- app fields
            FIRST_VALUE(app_info_id IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS app_info_id,
            FIRST_VALUE(app_info_version IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS app_info_version

    FROM pageviews_sessionized
),

diffs AS (
    SELECT  *,
            DATETIME_DIFF(CAST(session_end_tstamp AS DATETIME), CAST(session_start_tstamp AS DATETIME), second) AS engaged_time_in_s
    FROM agg
),

tiers AS (
    SELECT  *,
            CASE
                WHEN engaged_time_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
                WHEN engaged_time_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
                WHEN engaged_time_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
                WHEN engaged_time_in_s > 59 THEN '60s or more'
                ELSE NULL END AS engaged_time_in_s_tier
    FROM diffs
),

mapped AS (
    SELECT  tiers.*,
            source_categories.source_category as traffic_source_source_category
    FROM tiers
    LEFT JOIN source_categories ON LOWER(tiers.traffic_source_source) = LOWER(source_categories.source)
)

SELECT * FROM mapped
