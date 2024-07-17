{{ config(
    materialized='table'
) }}

-- Step 1: Extract relevant fields from the staging events data
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
        traffic_name  
    from {{ ref('stg_events') }} -- Reference to the staging events table
),

-- Step 2: Aggregate metrics by event date
aggregated_data as (
    select
        event_date,
        session_id,
        user_pseudo_id,
        user_first_touch_timestamp,
        event_timestamp,
        event_name,
        -- Count distinct session IDs to get total sessions per event date
        count(distinct session_id) over (partition by event_date) as total_sessions,
        -- Count distinct user pseudo IDs to get total users per event date
        count(distinct user_pseudo_id) over (partition by event_date) as total_users,
        -- Count distinct new users based on the timestamp of their first touch
        count(distinct case when TIMESTAMP_MICROS(user_first_touch_timestamp) = TIMESTAMP_MICROS(event_timestamp) then user_pseudo_id else null end) over (partition by event_date) as total_new_users,
        -- Count page view events per event date
        count(case when event_name = 'page_view' then 1 else null end) over (partition by event_date) as total_page_views,
        -- Count search session events per event date
        count(case when event_name = 'view_search_results' then 1 else null end) over (partition by event_date) as total_sessions_with_search
    from source_data
),

-- Step 3: Calculate session durations in seconds for each user on each event date
session_durations as (
    select
        event_date,
        user_pseudo_id,
        -- Calculate the duration of each session in seconds
        TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) as session_duration
    from source_data
    group by event_date, user_pseudo_id
),

-- Step 4: Calculate the average session duration per event date
average_session_duration as (
    select
        event_date,
        AVG(session_duration) as session_avg_duration
    from session_durations
    group by event_date
)

-- Step 5: Select and combine the final aggregated metrics with average session duration
select
    ad.event_date,
    -- Use MAX to get the total counts per event date
    MAX(ad.total_sessions) as total_sessions,
    MAX(ad.total_users) as total_users,
    MAX(ad.total_new_users) as total_new_users,
    MAX(ad.total_page_views) as total_page_views,
    MAX(ad.total_sessions_with_search) as total_sessions_with_search,
    -- Include average session duration
    asd.session_avg_duration
from aggregated_data ad
-- Left join with average session duration data
left join average_session_duration asd on ad.event_date = asd.event_date
-- Group by event date and session average duration
group by ad.event_date, asd.session_avg_duration
