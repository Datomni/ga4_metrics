{{ config(materialized='incremental') }}

WITH source AS (
    SELECT *,
           page_referrer as referrer 
    FROM {{ ref('ga4_metrics__events_flattened') }}
    
    {% if is_incremental() %}
                
    WHERE event_timestamp_utc > (select max(event_timestamp_utc) from {{ this }} )

    {% endif %}
),

renamed AS (
    SELECT  user_pseudo_id,
            user_id,
            anonymous_id,
            event_timestamp_utc,
            page_title,
            page_location as page_url,
            safe_cast(
                split(
                    split(
                        replace(replace(replace(page_location, 'android-app://', ''), 'http://', ''), 'https://', ''), 
                    '/')[safe_offset(0)],
                '?')[safe_offset(0)] as string) as page_url_host,
            referrer, 
            replace(
                safe_cast(
                    split(
                        split(
                            replace(replace(replace(referrer, 'android-app://', ''), 'http://', '') ,'https://', ''),
                        '/')[safe_offset(0)],
                    '?')[safe_offset(0)] as string),
                'www.', '') as referrer_host,
            s.source as utm_source,
            medium as utm_medium,
            campaign as utm_campaign,
            term as utm_term,
            gclid,
            device_operating_system	as device,
            device_category
    FROM source s
)

SELECT * FROM renamed
