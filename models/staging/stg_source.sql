{{ config(
  materialized='table'
) }}

-- events_cleaned.sql in dbt models
WITH base_data AS (
  SELECT
    SAFE_CAST(user_pseudo_id AS STRING) AS user_pseudo_id,
    SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
    lower(SAFE_CAST(event_name AS STRING)) AS event_name,
    PARSE_DATE('%Y%m%d', event_date) AS event_date, -- Converts string to date for other uses
    CASE
      WHEN lower(SAFE_CAST(device.category AS STRING)) IN ('desktop', 'mobile', 'tablet') THEN lower(SAFE_CAST(device.category AS STRING))
      ELSE 'other'  -- Default value for unmatched categories
    END AS device_category,
    lower(SAFE_CAST(traffic_source.medium AS STRING)) AS traffic_medium,
    lower(SAFE_CAST(traffic_source.source AS STRING)) AS traffic_source,
    lower(SAFE_CAST(traffic_source.name AS STRING)) AS traffic_name,
    SAFE_CAST(event_params[SAFE_OFFSET(0)].value.string_value AS STRING) AS ga_session_number, -- Correctly referring to ga_session_number
    lower(SAFE_CAST(geo.country AS STRING)) AS country,
    lower(SAFE_CAST(geo.region AS STRING)) AS region,
    lower(SAFE_CAST(geo.city AS STRING)) AS city
  FROM
    {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
),

event_data AS (
  SELECT
    *,
    CONCAT(user_pseudo_id, '-', ga_session_number, '-', CAST(event_timestamp AS STRING)) AS session_id -- Consistently using ga_session_number here
  FROM
    base_data
  WHERE
    user_pseudo_id IS NOT NULL AND
    ga_session_number IS NOT NULL AND -- Corrected the reference here
    event_timestamp IS NOT NULL
)

SELECT
  user_pseudo_id,
  session_id,
  event_timestamp,
  event_name,
  event_date,
  traffic_medium,
  traffic_source,
  traffic_name,
  device_category,
  country,
  region,
  city
FROM
  event_data
