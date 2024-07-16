-- Get the data from source data
with stg_session_source as (
    select
        *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
),
-- select relevant columns from source data
stg_events as (
    select
        user_pseudo_id,
        CAST(event_date AS DATE) as event_date,
        event_timestamp,
        event_previous_timestamp,
        event_name,
        user_first_touch_timestamp,
        case
            when lower(device.category) in ('desktop', 'mobile', 'tablet') then lower(device.category) else 'unknown'
        end as device_category,
        geo.country,
        geo.region,
        geo.city,
        traffic_source.medium,
        traffic_source.source,
        traffic_source.name,
        -- Create a unique session_id by combining user_pseudo_id and event_timestamp
        user_pseudo_id || '-' || event_timestamp as session_id
    from stg_session_source
    where user_pseudo_id is not null -- Exclude rows with null user_pseudo_id
        and event_timestamp is not null -- Exclude rows with null event_timestamp
)
-- Displaying the source data
select * from stg_events
