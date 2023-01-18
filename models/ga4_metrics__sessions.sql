with sessions as (
    SELECT * 
    FROM {{ ref('ga4_metrics__sessions_stitched') }}
),

windowed AS (
    SELECT  *,
            row_number() over (partition by user_pseudo_id order by sessions.session_start_tstamp) as session_number
    FROM sessions
)

SELECT * FROM windowed
