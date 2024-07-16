{{ config(
    materialized='table'
) }}


with stg_session_source as (
    select 
    *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
),

stg_events as (
    select 
        user_pseudo_id,
        CAST(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', event_date)) AS DATE) as event_date,
        event_timestamp,
        event_previous_timestamp,
        lower(event_name) AS event_name,
        user_first_touch_timestamp,
        case
            when lower(device.category) in ('desktop', 'mobile', 'tablet') then lower(device.category)
            else 'unknown'
        end as device_category,
        geo.country,
        geo.region,
        geo.city,
        traffic_source.medium,
        traffic_source.source,
        traffic_source.name,
        user_pseudo_id || '-' || event_timestamp as session_id 
    from stg_session_source
    where user_pseudo_id is not null  
        and event_timestamp is not null  
)

select * from stg_events


