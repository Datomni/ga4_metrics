WITH pageviews AS (
    SELECT * 
    FROM {{ ref('ga4_metrics__page_views') }}
),

numbered AS (
    --This CTE is responsible for assigning an all-time page view number for a
    --given user_pseudo_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.
    SELECT  *,
            row_number() over (partition by user_pseudo_id order by event_timestamp_utc) as page_view_number
    FROM pageviews
),

lagged AS (
    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.
    SELECT  *,
            lag(event_timestamp_utc) over (partition by user_pseudo_id order by page_view_number) as previous_tstamp
    FROM numbered
),

diffed AS (
    --This CTE simply calculates `period_of_inactivity`.
    SELECT  *,
            datetime_diff(cast(event_timestamp_utc as datetime), cast(previous_tstamp as datetime), second) as period_of_inactivity
    FROM lagged
),

new_sessions AS (
    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.
    SELECT *,
            case when period_of_inactivity <= 30 * 60 then 0 else 1 end as new_session
    FROM diffed
),

session_numbers AS (
    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.
    SELECT  *,
            sum(new_session) over (partition by user_pseudo_id order by page_view_number
                rows between unbounded preceding and current row) as session_number
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
            to_hex(md5(cast(coalesce(cast(user_pseudo_id as string), '') || '-' || coalesce(cast(session_number as string), '') as string))) as session_id,
    FROM session_numbers
)

select * from session_ids
