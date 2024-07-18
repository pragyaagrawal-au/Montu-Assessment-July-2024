-- Configures the output of the SQL model to materialize as a table in the warehouse and to cluster data by the event_date for optimized query performance
{{ config(
  materialized='table',
  cluster_by=['event_date']
) }}

-- Defined a CTE named 'sessions' that computes session-level metrics
WITH sessions AS (
  SELECT
    session_id, 
    user_pseudo_id,  
    event_date,  
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,  -- Determines the start time of each session
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,  -- Determines the end time of each session
    COUNT(*) AS total_events,  -- Counts the total number of events in each session
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS page_views,  -- Counts unique page views per session
    MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_new_user  -- Determines if the session was from a new user
  FROM
    {{ ref('stg_source') }} 
  GROUP BY
    session_id,
    user_pseudo_id,
    event_date
),

-- Define another CTE named 'aggregated_metrics' for daily aggregated metrics
aggregated_metrics AS (
  SELECT
    event_date,  
    COUNT(DISTINCT session_id) AS total_sessions,  -- Counts the distinct number of sessions per day
    COUNT(DISTINCT user_pseudo_id) AS total_users,  -- Counts the distinct number of users per day
    SUM(is_new_user) AS total_new_users,  -- Sums the new user flags to get total new users per day
    SUM(page_views) AS total_page_views,  -- Sums all page views per day
    AVG(TIMESTAMP_DIFF(session_end, session_start, SECOND)) AS average_session_duration_seconds  -- Calculates the average duration of sessions in seconds
  FROM
    sessions
  GROUP BY
    event_date
)

-- Final SELECT statement to retrieve daily metrics from the 'aggregated_metrics' CTE
SELECT
  event_date,  
  total_sessions, 
  total_users,  
  total_new_users, 
  total_page_views, 
  average_session_duration_seconds 
FROM
  aggregated_metrics
