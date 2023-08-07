--TODO: use blended user_id instead of user_pseudo_id?
WITH conversion_src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__session_conversions') }}
    WHERE converted_flag = 1
),

session_src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions') }}
),

converted_sessions AS (
    SELECT ss.session_start_tstamp,
           CASE WHEN ss.traffic_source_medium IS NULL OR ss.traffic_source_medium = '(none)' THEN 'organic'
                WHEN ss.traffic_source_medium = 'cpc' THEN 'paid'
                WHEN ss.traffic_source_medium IN (SELECT * FROM UNNEST({{ var("traffic_source_medium_types") }})) THEN ss.traffic_source_medium
                ELSE 'other' END AS traffic_medium,
    FROM conversion_src cc
    JOIN session_src ss
    USING (user_pseudo_id, session_id)
)

SELECT
    DATE(DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}")) AS date,
    {% for medium in var('traffic_source_medium_types') %}
        (sum(case when LOWER(traffic_medium) = '{{ medium }}' then 1 else 0 end)) as {{ medium | replace(' ', '_') }}_visitor_conversions
    {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM converted_sessions
GROUP BY date
