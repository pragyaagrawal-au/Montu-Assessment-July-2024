{{ config(
  materialized='table'
) }}

-- Step 1: Source the raw data from GA4 obfuscated sample ecommerce events
with stg_session_source as (
    select 
        *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
),

-- Step 2: Transform the raw events data
stg_events as (
    select 
        user_pseudo_id,
        -- Convert event_date from string to DATE type
        CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS DATE) as event_date,
        event_timestamp,
        event_previous_timestamp,
        -- Standardize event_name to lowercase
        lower(event_name) AS event_name,
        user_first_touch_timestamp,
        -- Categorize device types, setting 'unknown' for any other categories
        case
            when lower(device.category) in ('desktop', 'mobile', 'tablet') then lower(device.category)
            else 'unknown'
        end as device_category,
        -- Standardize geographic information to lowercase
        lower(geo.country) as country,
        lower(geo.region) as region,
        lower(geo.city) as city,
        -- Standardize traffic source information to lowercase
        lower(traffic_source.medium) as traffic_medium,
        lower(traffic_source.source) as traffic_source,
        lower(traffic_source.name) as traffic_name,
        -- Create a unique session_id by combining user_pseudo_id and event_timestamp
        user_pseudo_id || '-' || event_timestamp as session_id 
    from stg_session_source
    -- Filter out records with null user_pseudo_id or event_timestamp
    where user_pseudo_id is not null  
        and event_timestamp is not null  
)

-- Step 3: Select all transformed data
select * from stg_events
