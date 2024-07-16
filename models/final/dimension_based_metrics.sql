{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        session_id,
        event_date,
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
    FROM {{ ref('stg_events') }}
),

session_durations AS (
    SELECT
        event_date,
        user_pseudo_id,
        TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS duration_seconds
    FROM source_data
    GROUP BY event_date, user_pseudo_id
),

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
        sd.traffic_medium,  -- Make sure alias is correctly defined here
        sd.traffic_source,
        sd.traffic_name,
        COUNT(DISTINCT sd.session_id) AS session_count,
        COUNT(DISTINCT sd.user_pseudo_id) AS user_count,
        COUNT(DISTINCT IF(TIMESTAMP_MICROS(sd.user_first_touch_timestamp) = TIMESTAMP_MICROS(sd.event_timestamp), sd.user_pseudo_id, NULL)) AS new_user_count,
        COUNT(IF(sd.event_name = 'page_view', 1, NULL)) AS page_view_count,
        COUNT(IF(sd.event_name = 'view_search_results', 1, NULL)) AS search_session_count,
        AVG(sess.duration_seconds) AS average_session_duration
    FROM source_data sd
    LEFT JOIN session_durations sess ON sess.event_date = sd.event_date AND sess.user_pseudo_id = sd.user_pseudo_id
    GROUP BY sd.year, sd.month, sd.day, sd.event_date, sd.device_category, sd.country, sd.region, sd.city, sd.traffic_medium, sd.traffic_source, sd.traffic_name
)

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
    COALESCE(session_count, 0) AS session_count,
    COALESCE(user_count, 0) AS user_count,
    COALESCE(new_user_count, 0) AS new_user_count,
    COALESCE(page_view_count, 0) AS page_view_count,
    COALESCE(search_session_count, 0) AS search_session_count,
    COALESCE(average_session_duration, 0) AS average_session_duration
FROM aggregated_metrics
ORDER BY session_count DESC,
         user_count DESC,
         page_view_count DESC,
         new_user_count DESC,
         search_session_count DESC,
         average_session_duration DESC
