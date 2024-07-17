{{ config(
    materialized='table'
) }}

-- Step 1: Extract relevant fields from the staging events data
WITH source_data AS (
    SELECT
        session_id,
        event_date,
        -- Extract year, month, and day from event_date
        EXTRACT(YEAR FROM event_date) AS year,
        EXTRACT(MONTH FROM event_date) AS month,
        EXTRACT(DAY FROM event_date) AS day,
        event_timestamp,
        user_first_touch_timestamp,
        event_previous_timestamp,
        event_name,
        user_pseudo_id,
        device_category,
        country,
        region,
        city,
        traffic_medium,
        traffic_source,
        traffic_name
    FROM {{ ref('stg_events') }} -- Reference to the staging events table
),

-- Step 2: Calculate session durations in seconds
session_durations AS (
    SELECT
        event_date,
        user_pseudo_id,
        -- Calculate the duration of each session in seconds
        TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS duration_seconds
    FROM source_data
    GROUP BY event_date, user_pseudo_id
),

-- Step 3: Aggregate metrics by various dimensions
aggregated_metrics AS (
    SELECT
        sd.event_date,
        sd.year,
        sd.month,
        sd.day,
        sd.device_category,
        sd.country,
        sd.region,
        sd.city,
        sd.traffic_medium,
        sd.traffic_source,
        sd.traffic_name,
        -- Count distinct session IDs to get session count
        COUNT(DISTINCT sd.session_id) AS session_count,
        -- Count distinct user pseudo IDs to get user count
        COUNT(DISTINCT sd.user_pseudo_id) AS user_count,
        -- Count distinct new users based on the timestamp of their first touch
        COUNT(DISTINCT IF(TIMESTAMP_MICROS(sd.user_first_touch_timestamp) = TIMESTAMP_MICROS(sd.event_timestamp), sd.user_pseudo_id, NULL)) AS new_user_count,
        -- Count page view events
        COUNT(IF(sd.event_name = 'page_view', 1, NULL)) AS page_view_count,
        -- Count search session events
        COUNT(IF(sd.event_name = 'view_search_results', 1, NULL)) AS search_session_count,
        -- Calculate average session duration
        AVG(sess.duration_seconds) AS average_session_duration
    FROM source_data sd
    LEFT JOIN session_durations sess ON sess.event_date = sd.event_date AND sess.user_pseudo_id = sd.user_pseudo_id -- Join with session durations
    GROUP BY 
        sd.year, sd.month, sd.day, 
        sd.event_date, sd.device_category, 
        sd.country, sd.region, sd.city, 
        sd.traffic_medium, sd.traffic_source, sd.traffic_name -- Group by all the required dimensions
)

-- Step 4: Select and order the final aggregated metrics
SELECT
    year,
    month,
    day,
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name,
    -- Use COALESCE to replace null values with 0
    COALESCE(session_count, 0) AS session_count,
    COALESCE(user_count, 0) AS user_count,
    COALESCE(new_user_count, 0) AS new_user_count,
    COALESCE(page_view_count, 0) AS page_view_count,
    COALESCE(search_session_count, 0) AS search_session_count,
    COALESCE(average_session_duration, 0) AS average_session_duration
FROM aggregated_metrics
-- Order the results by various metrics in descending order
ORDER BY session_count DESC,
         user_count DESC,
         page_view_count DESC,
         new_user_count DESC,
         search_session_count DESC,
         average_session_duration DESC
