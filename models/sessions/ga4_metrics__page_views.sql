{{ config(materialized='incremental') }}

--TODO: handle late arriving events
WITH src AS (
    SELECT *
    FROM {{ ref('ga4_metrics__events_flattened') }}
    
    {% if is_incremental() %}
                
    WHERE event_timestamp_utc > (select max(event_timestamp_utc) from {{ this }} )

    {% endif %}
),

--TODO: handle duplicate events
renamed AS (
    SELECT
            -- user fields
            anonymous_id,
            user_id,
            user_pseudo_id,

            -- session fields
            ga_session_id  as session_id,
            ga_session_number  as session_number,

            -- timestamp fields
            event_date,
            event_timestamp,
            event_timestamp_utc,
            user_first_touch_timestamp,

            -- page fields
            page_title,
            page_location as page_url,
            safe_cast(
                split(
                    split(
                        replace(replace(replace(page_location, 'android-app://', ''), 'http://', ''), 'https://', ''),
                    '/')[safe_offset(0)],
                '?')[safe_offset(0)] as string) as page_url_host,


            -- referrer fields
            page_referrer as referrer,
            replace(
                safe_cast(
                    split(
                        split(
                            replace(replace(replace(page_referrer, 'android-app://', ''), 'http://', '') ,'https://', ''),
                        '/')[safe_offset(0)],
                    '?')[safe_offset(0)] as string),
                'www.', '') as referrer_host,

            -- mkt fields/traffic source
            source as utm_source,
            medium as utm_medium,
            campaign as utm_campaign,
            term as utm_term,
            gclid,
            traffic_source_name,
            traffic_source_medium,
            traffic_source_source,
            platform,

            -- geo fields
            geo_continent,
            geo_country,
            geo_region,
            geo_city,
            geo_sub_continent,
            geo_metro,

            -- device fields
            device_category,
            device_mobile_brand_name,
            device_mobile_model_name,
            device_operating_system,
            device_operating_system_version,
            device_vendor_id,
            device_advertising_id,
            device_language,
            device_browser,
            device_browser_version,
            device_web_info_browser,
            device_web_info_browser_version,
            device_web_info_hostname,

            -- app fields
            app_info_id,
            app_info_version

    FROM src
    WHERE ga_session_id IS NOT NULL AND
          event_name = 'page_view'
)

SELECT *,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp_utc) AS page_view_number
FROM renamed
