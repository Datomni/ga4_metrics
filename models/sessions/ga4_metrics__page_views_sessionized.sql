WITH pageviews AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views') }}
),

numbered AS (
    --This CTE is responsible for assigning an all-time page view number for a
    --given user_pseudo_id.
    SELECT  *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp_utc) AS page_view_number
    FROM pageviews
),

lagged AS (
    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.
    SELECT  *,
            LAG(event_timestamp_utc) OVER (PARTITION BY user_pseudo_id ORDER BY page_view_number) AS previous_tstamp
    FROM numbered
),

diffed AS (
    --This CTE simply calculates `period_of_inactivity`.
    SELECT  *,
            datetime_diff(CAST(event_timestamp_utc AS DATETIME), CAST(previous_tstamp AS DATETIME), second) AS period_of_inactivity
    FROM lagged
),

new_sessions AS (
    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.
    SELECT *,
            CASE WHEN period_of_inactivity <= 30 * 60 THEN 0 ELSE 1 END AS new_session
    FROM diffed
),

session_numbers AS (
    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.
    SELECT  *,
            SUM(new_session) OVER (PARTITION BY user_pseudo_id ORDER BY page_view_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number
    FROM new_sessions
),

session_ids AS (
    --This CTE assigns a globally unique session id based on the combination of
    --`user_pseudo_id` and `session_number`.
    SELECT  user_pseudo_id,
            user_id,
            anonymous_id,
            event_timestamp_utc,
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
            TO_HEX(MD5(CAST(COALESCE(CAST(user_pseudo_id AS string), '') || '-' || COALESCE(CAST(session_number AS string), '') AS string))) AS session_id,
    FROM session_numbers
)

SELECT * 
FROM session_ids
