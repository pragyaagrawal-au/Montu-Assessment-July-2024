{{ config(
  materialized='table',
  cluster_by=['event_date']
) }}

WITH sessions AS (
  SELECT
    session_id,
    user_pseudo_id,
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,
    COUNT(*) AS total_events,
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS page_views,
    MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_new_user
  FROM
    {{ ref('stg_source') }}
  GROUP BY
    session_id,
    user_pseudo_id,
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name
),

dimensional_aggregated_metrics AS (
  SELECT
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    SUM(is_new_user) AS total_new_users,
    SUM(page_views) AS total_page_views,
    AVG(TIMESTAMP_DIFF(session_end, session_start, SECOND)) AS average_session_duration_seconds
  FROM
    sessions
  GROUP BY
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name
)

SELECT
  event_date,
  device_category,
  country,
  region,
  city,
  traffic_medium,
  traffic_source,
  traffic_name,
  total_sessions,
  total_users,
  total_new_users,
  total_page_views,
  average_session_duration_seconds
FROM
  dimensional_aggregated_metrics
