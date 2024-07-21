-- Common Table Expression (CTE) to prepare base data from the Google Analytics 4 dataset
WITH base_data AS (
  SELECT
    user_pseudo_id,  
    event_timestamp,  
    LOWER(SAFE_CAST(event_name AS STRING)) AS event_name,  -- Normalizes event names to lowercase for uniformity
    PARSE_DATE('%Y%m%d', event_date) AS event_date,  -- Converts STRING date to DATE format for easier date operations

    (CASE
      WHEN LOWER(SAFE_CAST(device.category AS STRING)) IN ('desktop', 'mobile', 'tablet')
        THEN LOWER(SAFE_CAST(device.category AS STRING))  -- Filters and normalizes device categories
      ELSE 'other'  -- Groups all other device types into 'other' category for simplified analysis
    END) AS device_category,

    (CASE
    WHEN LOWER(SAFE_CAST(traffic_source.medium AS STRING)) IN ('cpc', 'organic', 'referral') THEN LOWER(SAFE_CAST(traffic_source.medium AS STRING))
    WHEN LOWER(SAFE_CAST(traffic_source.medium AS STRING)) IN ('<Other>') THEN 'other'
    WHEN LOWER(SAFE_CAST(traffic_source.medium AS STRING)) = '(data deleted)' THEN 'deleted'
    WHEN LOWER(SAFE_CAST(traffic_source.medium AS STRING)) = '(direct)' THEN 'direct'
    ELSE 'unknown' 
    END) AS traffic_medium,  

    (CASE
    WHEN LOWER(SAFE_CAST(traffic_source.source AS STRING)) IN ('google', 'shop.googlemerchandisestore.com') THEN LOWER(SAFE_CAST(traffic_source.source AS STRING))
    WHEN LOWER(SAFE_CAST(traffic_source.source AS STRING)) = '(direct)' THEN 'direct'
    WHEN LOWER(SAFE_CAST(traffic_source.source AS STRING)) = '(data deleted)' THEN 'deleted'
    WHEN LOWER(SAFE_CAST(traffic_source.source AS STRING)) = '<Other>' THEN 'other'
    ELSE 'unknown' 
    END) AS traffic_source,  

    (CASE
    WHEN LOWER(SAFE_CAST(traffic_source.name AS STRING)) IN ('organic') THEN LOWER(SAFE_CAST(traffic_source.name AS STRING))
    WHEN LOWER(SAFE_CAST(traffic_source.name AS STRING)) IN ('<Other>') THEN 'other'
    WHEN LOWER(SAFE_CAST(traffic_source.name AS STRING)) = '(data deleted)' THEN 'deleted'
    WHEN LOWER(SAFE_CAST(traffic_source.name AS STRING)) = '(direct)' THEN 'direct'
    ELSE 'unknown' 
    END) AS traffic_name,

    (SELECT SAFE_CAST(ep.value.int_value AS STRING) FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id') AS ga_session_number,  -- Extracts the GA session ID from event parameters
    LOWER(SAFE_CAST(geo.country AS STRING)) AS country,  -- Fetches country name from GEO data
    LOWER(SAFE_CAST(geo.region AS STRING)) AS region, 
    LOWER(SAFE_CAST(geo.city AS STRING)) AS city 
  FROM
    {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}  
),

-- CTE to append session IDs to each event for unique session identification
event_data AS (
  SELECT
    *,
    user_pseudo_id || ga_session_number || event_timestamp || CAST(ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, ga_session_number, event_timestamp ORDER BY event_timestamp) AS STRING) AS session_id 
    -- Concatenates user ID, session number, and timestamp to form a unique session ID for each user session
  FROM
    base_data
  WHERE
    user_pseudo_id IS NOT NULL AND
    ga_session_number IS NOT NULL AND
    event_timestamp IS NOT NULL  -- Ensures data integrity by filtering out records with null essential identifiers
)

-- Final query to select all relevant fields
SELECT
  user_pseudo_id,
  session_id,
  ga_session_number,
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
