WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions_stitched') }}
),

organic_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, '{{var("timezone")}}') AS session_start_tstamp,
            user_pseudo_id,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE 
        (first_page_url_host <> 'nuxt-corp.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:8101' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000guides' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> ':' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'http' OR first_page_url_host IS NULL) AND 
        -- (first_page_url_host <> 'user-api.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        
        (last_page_url_host <> ':' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'http' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000guides' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:8101' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'nuxt-corp.telemetrytv.com' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'user-api.telemetrytv.com' OR last_page_url_host IS NULL) AND 
        
        duration_in_s >= 5 AND 
        
        -- add earned media too???
        (LOWER(utm_medium) NOT IN ('cpc', 'social') OR utm_medium IS NULL)
),

-- unique users per month
organic_traffice_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_organic_traffic_unique
    FROM organic_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

paid_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, '{{var("timezone")}}') AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE 
        LOWER(utm_medium) = 'cpc' AND 
        
        -- (first_page_url_host <> 'user-api.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        -- (first_page_url_host <> 'nuxt-corp.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:8101' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000guides' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'http' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> ':' OR first_page_url_host IS NULL) AND 
        
        (last_page_url_host <> ':' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'http' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000guides' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:8101' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'nuxt-corp.telemetrytv.com' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'user-api.telemetrytv.com' OR last_page_url_host IS NULL) AND

        duration_in_s >= 5 
),

-- unique users per month
paid_traffice_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_paid_traffic_unique
    FROM paid_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

social_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, '{{var("timezone")}}') AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE 
        LOWER(utm_medium) = 'social' AND 
        
        -- (first_page_url_host <> 'user-api.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        -- (first_page_url_host <> 'nuxt-corp.telemetrytv.com' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:8101' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000guides' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'localhost:3000' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> 'http' OR first_page_url_host IS NULL) AND 
        (first_page_url_host <> ':' OR first_page_url_host IS NULL) AND 
        
        (last_page_url_host <> ':' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'http' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:3000guides' OR last_page_url_host IS NULL) AND 
        (last_page_url_host <> 'localhost:8101' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'nuxt-corp.telemetrytv.com' OR last_page_url_host IS NULL) AND 
        -- (last_page_url_host <> 'user-api.telemetrytv.com' OR last_page_url_host IS NULL) AND

        duration_in_s >= 5 
),

-- unique users per month
social_traffice_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_social_traffic_unique
    FROM social_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
)

SELECT date,
       COALESCE(uv_organic_traffic_unique, 0) AS uv_organic_traffic_unique,
       COALESCE(uv_paid_traffic_unique, 0) AS uv_paid_traffic_unique,
       COALESCE(uv_social_traffic_unique, 0) AS uv_social_traffic_unique
FROM organic_traffice_unique
FULL OUTER JOIN paid_traffice_unique USING (date)
FULL OUTER JOIN social_traffice_unique USING (date)
