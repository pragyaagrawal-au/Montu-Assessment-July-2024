SELECT
  session_id,
  COUNT(*) AS duplicate_count
FROM
  {{ ref('stg_source') }}
GROUP BY
  session_id
HAVING
  COUNT(*) > 1
ORDER BY
  duplicate_count DESC

-- select * from {{ ref('stg_source') }} where session_id = '25630609.407091673233674340621611978194880155'

-- SELECT
--     SAFE_CAST(user_pseudo_id AS STRING) AS user_pseudo_id,
--     SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
--     lower(SAFE_CAST(event_name AS STRING)) AS event_name,
--     PARSE_DATE('%Y%m%d', event_date) AS event_date,
--     CASE
--       WHEN lower(SAFE_CAST(device.category AS STRING)) IN ('desktop', 'mobile', 'tablet') THEN lower(SAFE_CAST(device.category AS STRING))
--       ELSE 'other'  
--     END AS device_category,
--     lower(SAFE_CAST(traffic_source.medium AS STRING)) AS traffic_medium,
--     lower(SAFE_CAST(traffic_source.source AS STRING)) AS traffic_source,
--     lower(SAFE_CAST(traffic_source.name AS STRING)) AS traffic_name,
--     (SELECT SAFE_CAST(ep.value.int_value AS STRING) FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id') AS ga_session_number,
--     lower(SAFE_CAST(geo.country AS STRING)) AS country,
--     lower(SAFE_CAST(geo.region AS STRING)) AS region,
--     lower(SAFE_CAST(geo.city AS STRING)) AS city
--   FROM
--     {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}