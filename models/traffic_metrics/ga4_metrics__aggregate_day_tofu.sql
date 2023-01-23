WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__sessions_stitched') }}
),

filtered AS (
    SELECT *
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

organic_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp,
            user_pseudo_id,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'organic' OR utm_medium IS NULL
),

-- unique users per month
organic_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_organic_traffic_unique
    FROM organic_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

paid_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'cpc'
),

-- unique users per month
paid_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_paid_traffic_unique
    FROM paid_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

social_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'social' 
),

-- unique users per month
social_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_social_traffic_unique
    FROM social_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

earned_media_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'earned media' 
),

-- unique users per month
earned_media_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_earned_media_traffic_unique
    FROM earned_media_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

marketplace_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'marketplace' 
),

-- unique users per month
marketplace_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_marketplace_traffic_unique
    FROM marketplace_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
),

referral_traffic_tmp AS (
    SELECT DATETIME(session_start_tstamp, "{{ var('timezone', 'Etc/UCT') }}") AS session_start_tstamp, 
        user_pseudo_id,
        ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, DATE_TRUNC(session_start_tstamp, MONTH) ORDER BY session_start_tstamp) AS row_num
    FROM src
    WHERE LOWER(utm_medium) = 'referral' 
),

-- unique users per month
referral_traffic_unique AS (
    SELECT DATE(session_start_tstamp) AS date, 
            COUNT(DISTINCT user_pseudo_id) AS uv_referral_traffic_unique
    FROM referral_traffic_tmp
    WHERE row_num = 1
    GROUP BY date
)

SELECT date,
       COALESCE(uv_organic_traffic_unique, 0) AS uv_organic_traffic_unique,
       COALESCE(uv_paid_traffic_unique, 0) AS uv_paid_traffic_unique,
       COALESCE(uv_social_traffic_unique, 0) AS uv_social_traffic_unique,
       COALESCE(uv_earned_media_traffic_unique, 0) AS uv_earned_media_traffic_unique,
       COALESCE(uv_marketplace_traffic_unique, 0) AS uv_marketplace_traffic_unique,
       COALESCE(uv_referral_traffic_unique, 0) AS uv_referral_traffic_unique
FROM organic_traffic_unique
FULL OUTER JOIN paid_traffic_unique USING (date)
FULL OUTER JOIN social_traffic_unique USING (date)
FULL OUTER JOIN earned_media_traffic_unique USING (date)
FULL OUTER JOIN marketplace_traffic_unique USING (date)
FULL OUTER JOIN referral_traffic_unique USING (date)
