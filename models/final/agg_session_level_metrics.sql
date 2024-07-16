{{ config(
    materialized='table'
) }}

with source_data as (
    select
        session_id,
        event_date,
        event_timestamp,
        user_first_touch_timestamp,
        event_previous_timestamp,
        event_name,
        user_pseudo_id,
        device_category,
        country,
        name  
    from {{ ref('stg_events') }}
),

aggregated_data as (
    select
        event_date,
        session_id,
        user_pseudo_id,
        user_first_touch_timestamp,
        event_timestamp,
        event_name,
        count(distinct session_id) over (partition by event_date) as total_sessions,
        count(distinct user_pseudo_id) over (partition by event_date) as total_users,
        count(distinct case when TIMESTAMP_MICROS(user_first_touch_timestamp) = TIMESTAMP_MICROS(event_timestamp) then user_pseudo_id else null end) over (partition by event_date) as total_new_users,
        count(case when event_name = 'page_view' then 1 else null end) over (partition by event_date) as total_page_views,
        count(case when event_name = 'view_search_results' then 1 else null end) over (partition by event_date) as total_sessions_with_search
    from source_data
),

session_durations as (
    select
        event_date,
        user_pseudo_id,
        TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) as session_duration
    from source_data
    group by event_date, user_pseudo_id
),

average_session_duration as (
    select
        event_date,
        AVG(session_duration) as session_avg_duration
    from session_durations
    group by event_date
)

select
    ad.event_date,
    MAX(ad.total_sessions) as total_sessions,
    MAX(ad.total_users) as total_users,
    MAX(ad.total_new_users) as total_new_users,
    MAX(ad.total_page_views) as total_page_views,
    MAX(ad.total_sessions_with_search) as total_sessions_with_search,
    asd.session_avg_duration
from aggregated_data ad
left join average_session_duration asd on ad.event_date = asd.event_date
group by ad.event_date, asd.session_avg_duration
