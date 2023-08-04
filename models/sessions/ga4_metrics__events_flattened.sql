{{ config(materialized='incremental') }}

--TODO: handle late arriving event
WITH src AS (SELECT
                    ROW_NUMBER() OVER() AS uuid,
                    *
                FROM {{ ref('tmp_ga4_metrics__events')}}
                
                {% if is_incremental() %}
                
                WHERE event_timestamp > (select max(event_timestamp) from {{ this }} )

                {% endif %}
),

event_params_flattened AS (
    SELECT uuid,
            ep.key AS event_params_key,
            COALESCE(ep.value.string_value, CAST(ep.value.int_value AS string),
                    CAST(ep.value.float_value AS string), CAST(ep.value.double_value AS string)) AS event_params_value
      FROM src
      CROSS JOIN UNNEST(src.event_params) AS ep
),

user_properties_flattened AS (
    SELECT uuid,
          up.key AS user_properties_key,
          COALESCE(up.value.string_value, CAST(up.value.int_value AS string),
                    CAST(up.value.float_value AS string), CAST(up.value.double_value AS string)) AS user_properties_value,
          up.value.set_timestamp_micros AS user_properties_set_timestamp_micros
    FROM src
    CROSS JOIN UNNEST(src.user_properties) AS up
),

pivoted AS (SELECT uuid,
                  -- Default event params
                  MAX(IF(event_params_key = "page_title", event_params_value, NULL)) AS page_title,
                  MAX(IF(event_params_key = "ga_session_id", event_params_value, NULL)) AS ga_session_id,
                  MAX(IF(event_params_key = "ga_session_number", event_params_value, NULL)) AS ga_session_number,
                  MAX(IF(event_params_key = "page_location", event_params_value, NULL)) AS page_location,
                  MAX(IF(event_params_key = "page_referrer", event_params_value, NULL)) AS page_referrer,
                  MAX(IF(event_params_key = "term", event_params_value, NULL)) AS term,
                  MAX(IF(event_params_key = "medium", event_params_value, NULL)) AS medium,
                  MAX(IF(event_params_key = "entrances", event_params_value, NULL)) AS entrances,
                  MAX(IF(event_params_key = "gclid", event_params_value, NULL)) AS gclid,
                  MAX(IF(event_params_key = "source", event_params_value, NULL)) AS source,
                  MAX(IF(event_params_key = "campaign", event_params_value, NULL)) AS campaign,
                  MAX(IF(event_params_key = "session_engaged", event_params_value, NULL)) AS session_engaged,
                  MAX(IF(event_params_key = "engaged_session_event", event_params_value, NULL)) AS engaged_session_event,
                  MAX(IF(event_params_key = "percent_scrolled", event_params_value, NULL)) AS percent_scrolled,
                  MAX(IF(event_params_key = "engagement_time_msec", event_params_value, NULL)) AS engagement_time_msec,
                  MAX(IF(event_params_key = "ignore_referrer", event_params_value, NULL)) AS ignore_referrer,

                  -- Custom event params
                  {% if var("custom_event_parameters") != [] %}

                    {% for param in var('custom_event_parameters') %}
                        MAX(IF(event_params_key = "{{ param }}", event_params_value, NULL)) AS {{ param }}
                    {% if not loop.last %}, {% endif %}
                    {% endfor %}

                  {% endif %}

                  -- Default user_properties
                  MAX(IF(user_properties_key = "anonymousId", user_properties_value, NULL)) AS anonymous_id,
                  MAX(IF(user_properties_key = "anonymousId", user_properties_set_timestamp_micros, NULL)) AS anonymous_id_set_timestamp_micros

                  -- Custom user_properties
                  {% if var("custom_user_props") != [] %}

                    {% for prop in var('custom_user_properties') %}
                    {% if loop.first %}, {% endif %}
                        ,MAX(IF(user_properties_key = "{{ prop }}", user_properties_value, NULL)) AS {{ prop }}
                    {% if not loop.last %}, {% endif %}
                    {% endfor %}

                  {% endif %}
            FROM event_params_flattened
            FULL OUTER JOIN user_properties_flattened
            USING (uuid)
            GROUP BY uuid)

SELECT s.event_date,
       s.event_timestamp,
       TIMESTAMP_MICROS(s.event_timestamp) AS event_timestamp_utc,
       s.event_name,
       p.*,
       s.event_previous_timestamp,
       s.event_value_in_usd,
       s.event_bundle_sequence_id,    
       s.event_server_timestamp_offset,
       s.user_id,
       s.user_pseudo_id,
       s.privacy_info.analytics_storage AS privacy_info_analytics_storage,
       s.privacy_info.ads_storage AS privacy_info_ads_storage,
       s.privacy_info.uses_transient_token AS privacy_info_uses_transient_token,
       s.user_first_touch_timestamp,
       s.user_ltv.revenue AS user_ltv_revenue,
       s.user_ltv.currency AS user_ltv_currency,
       s.device.category AS device_category,
       s.device.mobile_brand_name AS device_mobile_brand_name,
       s.device.mobile_model_name AS device_mobile_model_name,
       s.device.mobile_marketing_name AS device_mobile_marketing_name,
       s.device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
       s.device.operating_system AS device_operating_system,
       s.device.operating_system_version AS device_operating_system_version,  
       s.device.vendor_id AS device_vendor_id,
       s.device.advertising_id AS device_advertising_id,
       s.device.language AS device_language,
       s.device.is_limited_ad_tracking AS device_is_limited_ad_tracking,  
       s.device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
       s.device.browser AS device_browser,
       s.device.browser_version AS device_browser_version,  
       s.device.web_info.browser AS device_web_info_browser,
       s.device.web_info.browser_version AS device_web_info_browser_version,
       s.device.web_info.hostname AS device_web_info_hostname,
       s.geo.continent AS geo_continent,
       s.geo.country AS geo_country,
       s.geo.region AS geo_region,
       s.geo.city AS geo_city,
       s.geo.sub_continent AS geo_sub_continent,
       s.geo.metro AS geo_metro,
       s.app_info.id AS app_info_id,
       s.app_info.version AS app_info_version,
       s.app_info.install_store AS app_info_install_store,  
       s.app_info.firebase_app_id AS app_info_firebase_app_id,
       s.app_info.install_source AS app_info_install_source,
       s.traffic_source.name AS traffic_source_name,
       s.traffic_source.medium AS traffic_source_medium,
       s.traffic_source.source AS traffic_source_source,
       s.stream_id,
       s.platform,
       s.event_dimensions.hostname AS event_dimensions_hostname,
       s.ecommerce.total_item_quantity AS ecommerce_total_item_quantity,
       s.ecommerce.purchase_revenue_in_usd AS ecommerce_purchase_revenue_in_usd,
       s.ecommerce.purchase_revenue AS ecommerce_purchase_revenue,
       s.ecommerce.refund_value_in_usd AS ecommerce_refund_value_in_usd,
       s.ecommerce.refund_value AS ecommerce_refund_value,
       s.ecommerce.shipping_value_in_usd AS ecommerce_shipping_value_in_usd,
       s.ecommerce.shipping_value AS ecommerce_shipping_value, 
       s.ecommerce.tax_value_in_usd AS ecommerce_tax_value_in_usd,
       s.ecommerce.tax_value AS ecommerce_tax_value,
       s.ecommerce.unique_items AS ecommerce_unique_items,
       s.ecommerce.transaction_id AS ecommerce_transaction_id,
       s.items[SAFE_OFFSET(0)].item_id AS items_item_id,
       s.items[SAFE_OFFSET(0)].item_name AS items_item_name,
       s.items[SAFE_OFFSET(0)].item_brand AS items_item_brand,
       s.items[SAFE_OFFSET(0)].item_variant AS items_item_variant,
       s.items[SAFE_OFFSET(0)].item_category AS items_item_category,
       s.items[SAFE_OFFSET(0)].item_category2 AS items_item_category2,
       s.items[SAFE_OFFSET(0)].item_category3 AS items_item_category3,
       s.items[SAFE_OFFSET(0)].item_category4 AS items_item_category4,
       s.items[SAFE_OFFSET(0)].item_category5 AS items_item_category5,
       s.items[SAFE_OFFSET(0)].price_in_usd AS items_price_in_usd,
       s.items[SAFE_OFFSET(0)].price AS items_price,
       s.items[SAFE_OFFSET(0)].quantity AS items_quantity,
       s.items[SAFE_OFFSET(0)].item_revenue_in_usd AS items_item_revenue_in_usd,
       s.items[SAFE_OFFSET(0)].item_revenue AS items_item_revenue,
       s.items[SAFE_OFFSET(0)].item_refund_in_usd AS items_item_refund_in_usd,
       s.items[SAFE_OFFSET(0)].item_refund AS items_item_refund,  
       s.items[SAFE_OFFSET(0)].coupon AS items_coupon,  
       s.items[SAFE_OFFSET(0)].affiliation AS items_affiliation,
       s.items[SAFE_OFFSET(0)].location_id AS items_location_id,
       s.items[SAFE_OFFSET(0)].item_list_id AS items_item_list_id,
       s.items[SAFE_OFFSET(0)].item_list_name AS items_item_list_name,
       s.items[SAFE_OFFSET(0)].item_list_index AS items_item_list_index,
       s.items[SAFE_OFFSET(0)].promotion_id AS items_promotion_id,
       s.items[SAFE_OFFSET(0)].promotion_name AS items_promotion_name,
       s.items[SAFE_OFFSET(0)].creative_name AS items_creative_name,  
       s.items[SAFE_OFFSET(0)].creative_slot AS items_creative_slot
FROM src s
LEFT JOIN pivoted p USING (uuid)
