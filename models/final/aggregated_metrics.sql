-- Configures the output of the SQL model to materialize as a table in the warehouse and to cluster data by the event_date for optimized query performance
{{ config(
  materialized='table',
  PARTITION_BY = ['event_date']
) }}

-- Defined a CTE named 'sessions' that computes session-level metrics
WITH sessions AS (
  SELECT
    user_pseudo_id,  
    ga_session_number,
    event_date,  
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start,  -- Determines the start time of each session
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end,  -- Determines the end time of each session
    COUNT(*) AS total_events,  -- Counts the total number of events in each session
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS page_views,  -- Counts unique page views per session
    MAX(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS is_new_user,  -- Determines if the session was from a new user
    MAX(CASE WHEN LOWER(event_name) LIKE '%search%' THEN 1 ELSE 0 END) AS has_search
  FROM
    {{ ref('stg_source') }} 
  GROUP BY
    user_pseudo_id,
    ga_session_number,
    event_date
),

-- Define another CTE named 'aggregated_metrics' for daily aggregated metrics
aggregated_metrics AS (
  SELECT
    event_date, 
    COUNT(DISTINCT user_pseudo_id) AS total_users,  -- Counts the distinct number of users per day
    SUM(is_new_user) AS total_new_users,  -- Sums the new user flags to get total new users per day
    SUM(page_views) AS total_page_views,  -- Sums all page views per day
    round(AVG(TIMESTAMP_DIFF(session_end, session_start, SECOND)),2) AS average_session_duration_seconds,  -- Calculates the average duration of sessions in seconds
    SUM(has_search) AS total_sessions_with_search 
  FROM
    sessions
  GROUP BY
    event_date
),

total_events_metrics as (
    select 
    event_date,
    COUNT(DISTINCT session_id) AS total_sessions -- Counts the distinct number of sessions per day)
    from {{ ref('stg_source') }} 
  GROUP BY
    event_date
)
-- Final SELECT statement to retrieve daily metrics from the 'aggregated_metrics' CTE
SELECT
  am.event_date,  
  total_sessions,
  total_users,  
  total_new_users, 
  total_page_views, 
  total_sessions_with_search,
  average_session_duration_seconds
FROM
  aggregated_metrics am join total_events_metrics tem on am.event_date = tem.event_date
order by event_date
