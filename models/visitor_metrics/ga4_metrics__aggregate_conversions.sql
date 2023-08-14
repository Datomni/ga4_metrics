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
           -- categorise mediums using `traffic_source_medium_types` config variable
           CASE
                {% for medium in var('traffic_source_medium_types') %}
                        WHEN LOWER(traffic_source_medium) IN (SELECT * FROM UNNEST(  {{ var('traffic_source_medium_types')[medium] }})) THEN '{{ medium }}'
                {% endfor %}
                ELSE 'other' END AS traffic_medium,
    FROM conversion_src cc
    JOIN session_src ss
    USING (user_pseudo_id, session_id)
)

SELECT
    DATE(DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}")) AS date,
    {% for medium in var('traffic_source_medium_types') %}
        (sum(CASE WHEN LOWER(traffic_medium) = '{{ medium }}' THEN 1 ELSE 0 END)) AS {{ medium | replace(' ', '_') }}_visitor_conversions,
    {% endfor %}
    (sum(CASE WHEN LOWER(traffic_medium) = 'other' THEN 1 ELSE 0 END)) AS other_visitor_conversions
FROM converted_sessions
GROUP BY date
