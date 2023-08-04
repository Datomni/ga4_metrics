WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions') }}
),

filtered_sessions AS (
    SELECT *,
           -- mediums
           CASE WHEN traffic_source_medium IS NULL OR traffic_source_medium = '(none)' THEN 'organic'
                WHEN traffic_source_medium = 'cpc' THEN 'paid'
                WHEN traffic_source_medium IN (SELECT split({{ var("traffic_source_medium_types") }}) THEN traffic_source_medium
                ELSE 'other' END AS traffic_medium,
           -- unique users per month
           ROW_NUMBER() OVER (PARTITION BY domain_userid, {{ dbt.date_trunc('month', 'session_start_tstamp') }}
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
          duration_in_s >= 5 
),


SELECT
    DATE(DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}")) AS date,
    {% for medium in var('traffic_source_medium_types') %}
        (sum(CASE WHEN LOWER(traffic_medium) = '{{ medium }}' THEN 1 ELSE 0 END)) AS {{ medium | replace(' ', '_') }}_traffic_unique
    {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM filtered_sessions
WHERE row_num = 1
GROUP BY date
