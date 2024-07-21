-- Configures the output of the SQL model to materialize as a table in the warehouse and to cluster data by the 'event_date' for optimized query performance.
{{ config(
  materialized='table',
  PARTITION_BY = ['event_date']
) }}

-- Defined a CTE named 'sessions' that computes detailed session-level metrics including device and geographic dimensions.
WITH sessions AS (
  SELECT
    user_pseudo_id, 
    event_date, 
    device_category,  
    country,  
    region, 
    city,  
    traffic_medium,  
    traffic_source,  
    traffic_name, 
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,  -- Start time of the session
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,  -- End time of the session
    COUNT(*) AS total_events,  -- Total number of events during the session
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS page_views,  -- Counts unique page views during the session
    MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_new_user  -- Indicates if the session was from a new user
  FROM
    {{ ref('stg_source') }} 
  GROUP BY
    event_date,
    user_pseudo_id,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name
),

-- Define another CTE named 'dimensional_aggregated_metrics' to aggregate metrics at the dimension level for detailed analysis.
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
    COUNT(DISTINCT user_pseudo_id) AS total_users,  -- Total number of users per dimension
    SUM(is_new_user) AS total_new_users,  -- Total new users per dimension
    SUM(page_views) AS total_page_views,  -- Total page views per dimension
    AVG(TIMESTAMP_DIFF(session_end, session_start, SECOND)) AS average_session_duration_seconds  -- Average duration of sessions in seconds per dimension
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
),

dimensional_total_Sessions AS (
SELECT
    event_date,
    device_category,
    country,
    region,
    city,
    traffic_medium,
    traffic_source,
    traffic_name,
    COUNT(DISTINCT session_id) AS total_sessions,  -- Total number of sessions per dimension
FROM {{ ref('stg_source') }} 
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
-- Select and display the aggregated dimensional metrics from the 'dimensional_aggregated_metrics' CTE.
SELECT
  dam.event_date,
  dam.device_category,
  dam.country,
  dam.region,
  dam.city,
  dam.traffic_medium,
  dam.traffic_source,
  dam.traffic_name,
  total_sessions,
  dam.total_users,
  dam.total_new_users,
  dam.total_page_views,
  dam.average_session_duration_seconds
FROM
  dimensional_aggregated_metrics dam join dimensional_total_Sessions dts 
  on dam.event_date = dts.event_date and dam.device_category = dts.device_category
  and dam.country = dts.country