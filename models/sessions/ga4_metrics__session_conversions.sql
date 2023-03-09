WITH session_src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions_stitched') }}
),

conversion_src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__events_flattened') }}
),

conversions as (
    SELECT user_pseudo_id, 
          ga_session_id as session_id, 
          ga_session_number as session_number
    FROM
        (SELECT *,
               ROW_NUMBER() OVER (PARTITION BY user_pseudo_id  ORDER BY event_timestamp_utc ASC) AS row_num
        FROM conversion_src
        WHERE event_name = "{{ var('conversion_event_name') }}")
    WHERE row_num = 1
),

stitched_sessions AS (
    SELECT ss.user_pseudo_id,
           ss.session_id,
           ss.session_number,
           CASE WHEN c.user_pseudo_id IS NULL THEN 0 ELSE 1 END AS converted_count
    FROM session_src ss
    LEFT JOIN conversions c
    USING (user_pseudo_id, session_id, session_number)
)

SELECT DISTINCT
    user_pseudo_id,
    session_id,
    session_number,
    converted_count
FROM stitched_sessions
