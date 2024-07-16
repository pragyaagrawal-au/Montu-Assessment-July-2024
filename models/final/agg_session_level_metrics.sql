{{ config(
    materialized='table'
) }}

with event_data as (
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

total_sessions as (
    select
        event_date,
        count(distinct session_id) as total_sessions
    from event_data
    group by event_date
),

total_users as (
    select
        event_date,
        count(distinct user_pseudo_id) as total_users
    from event_data
    group by event_date
),

total_new_users as (
    select
        event_date,
        count(distinct user_pseudo_id) as total_new_users
    from event_data
    where TIMESTAMP_SECONDS(user_first_touch_timestamp) = TIMESTAMP_SECONDS(event_timestamp)
    group by event_date
),

total_page_views as (
    select
        event_date,
        count(*) as total_page_views
    from event_data
    where LOWER(event_name) = 'page_view'
    group by event_date
),

search_sessions as (
    select
        event_date,
        count(*) as total_sessions_with_search
    from event_data
    where LOWER(event_name) = 'view_search_results'
    group by event_date
),

session_durations as (
    select
        event_date,
        user_pseudo_id,
        MAX(TIMESTAMP_SECONDS(event_timestamp)) - MIN(TIMESTAMP_SECONDS(event_timestamp)) as session_duration
    from event_data
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
    ts.event_date,
    ts.total_sessions,
    tu.total_users,
    tnu.total_new_users,
    tpv.total_page_views,
    ss.total_sessions_with_search,
    sd.session_avg_duration
from total_sessions ts
left join total_users tu on ts.event_date = tu.event_date
left join total_new_users tnu on ts.event_date = tnu.event_date
left join total_page_views tpv on ts.event_date = tpv.event_date
left join search_sessions ss on ts.event_date = ss.event_date
left join average_session_duration sd on ts.event_date = sd.event_date
