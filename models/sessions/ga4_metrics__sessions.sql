with sessions as (
    SELECT * 
    FROM {{ ref('ga4_metrics__sessions_stitched') }}
),

windowed AS (
    SELECT  *,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY by session_start_tstamp) AS session_number
    FROM sessions
)

SELECT * FROM windowed
