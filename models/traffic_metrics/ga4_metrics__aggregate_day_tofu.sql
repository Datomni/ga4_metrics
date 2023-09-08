--TODO: add blended user_id to handle cookie deletes or same users accessing site from different devices
WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions') }}
),

filtered_sessions AS (
    SELECT *,
           -- categorise mediums using `traffic_source_medium_types` config variable
           CASE
                {% for medium in var('traffic_source_medium_types') %}
                        WHEN LOWER(traffic_source_medium) IN (SELECT * FROM UNNEST(  {{ var('traffic_source_medium_types')[medium] }})) THEN '{{ medium }}'
                {% endfor %}
                ELSE 'other' END AS traffic_medium,
           -- unique users per month
           ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, {{ dbt.date_trunc('month', 'session_start_tstamp') }}
                              ORDER BY session_start_tstamp ASC) AS row_num
    FROM src
    WHERE (first_page_url_host <> 'localhost:8101' OR first_page_url_host IS NULL) AND 
          (first_page_url_host <> 'localhost:3000guides' OR first_page_url_host IS NULL) AND 
          (first_page_url_host <> 'localhost:3000' OR first_page_url_host IS NULL) AND 
          (first_page_url_host <> ':' OR first_page_url_host IS NULL) AND 
          (first_page_url_host <> 'http' OR first_page_url_host IS NULL) AND 
        
          (last_page_url_host <> ':' OR last_page_url_host IS NULL) AND 
          (last_page_url_host <> 'http' OR last_page_url_host IS NULL) AND 
          (last_page_url_host <> 'localhost:3000' OR last_page_url_host IS NULL) AND 
          (last_page_url_host <> 'localhost:3000guides' OR last_page_url_host IS NULL) AND 
          (last_page_url_host <> 'localhost:8101' OR last_page_url_host IS NULL) AND
          engaged_time_in_s >= 5
)


SELECT
    DATE(DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}")) AS date,
    {% for medium in var('traffic_source_medium_types') %}
        (sum(CASE WHEN LOWER(traffic_medium) = '{{ medium }}' THEN 1 ELSE 0 END)) AS {{ medium | replace(' ', '_') }}_traffic_unique,
    {% endfor %}
    (sum(CASE WHEN LOWER(traffic_medium) = 'other' THEN 1 ELSE 0 END)) AS other_traffic_unique
FROM filtered_sessions
WHERE row_num = 1
GROUP BY date
