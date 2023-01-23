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
            min(event_timestamp_utc) over ( partition by session_id ) as session_start_tstamp,
            max(event_timestamp_utc) over ( partition by session_id ) as session_end_tstamp,
            count(distinct event_timestamp_utc) over ( partition by session_id ) as page_views,
            first_value(utm_source ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as utm_source,
            first_value(utm_medium ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as utm_medium,
            first_value(utm_campaign ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as utm_campaign,
            first_value(utm_term ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as utm_term,
            first_value(gclid ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as gclid,
            first_value(referrer ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as referrer,
            first_value(referrer_host ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as referrer_host,
            first_value(device ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as device,
            first_value(device_category ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as device_category,
            first_value(page_url ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as first_page_url,
            first_value(page_url_host ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as first_page_url_host,
            last_value(page_url ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as last_page_url,
            last_value(page_url_host ignore nulls) over (partition by session_id order by page_view_number
                rows between unbounded preceding and unbounded following) as last_page_url_host
    FROM pageviews_sessionized
),

diffs AS (
    SELECT  *,
            datetime_diff(cast(session_end_tstamp as datetime), cast(session_start_tstamp as datetime), second) as duration_in_s
    FROM agg
),

tiers AS (
    SELECT  *,
            case
                when duration_in_s between 0 and 9 then '0s to 9s'
                when duration_in_s between 10 and 29 then '10s to 29s'
                when duration_in_s between 30 and 59 then '30s to 59s'
                when duration_in_s > 59 then '60s or more'
                else null end as duration_in_s_tier
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
