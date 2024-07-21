-- SELECT
--     LOWER(SAFE_CAST(traffic_source.medium AS STRING)) AS traffic_medium,  
--     LOWER(SAFE_CAST(traffic_source.source AS STRING)) AS traffic_source,  
--     LOWER(SAFE_CAST(traffic_source.name AS STRING)) AS traffic_name, 
-- FROM
--     {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}  
-- -- where traffic_medium != traffic_source



-- select DISTINCT traffic_source.name as name, count(*) as count from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} group by traffic_source.name
-- union
-- select DISTINCT traffic_source.name as name, count(*) as count from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} group by traffic_source.name

-- SELECT
--     user_pseudo_id,  
--     event_date,  
--     MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,  -- Determines the start time of each session
--     MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,  -- Determines the end time of each session
--     COUNT(*) AS total_events,  -- Counts the total number of events in each session
--     COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS page_views,  -- Counts unique page views per session
--     MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_new_user  -- Determines if the session was from a new user
--   FROM
--     {{ ref('stg_source') }} 
--   GROUP BY
--     user_pseudo_id,
--     event_date

-- select DISTINCT traffic_source.medium from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}  
-- select DISTINCT traffic_source.name from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} 
-- select DISTINCT traffic_source.source from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} 
-- select DISTINCT device.category from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} 

-- select TIMESTAMP_MICROS(event_timestamp) from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} 

-- select * from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }} limit 5

-- SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id'