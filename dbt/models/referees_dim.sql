{{ config(materialized='table') }}

-- SELECT
--  -- DISTINCT(TRIM(REPLACE(name, '\u0000', ''))) AS name,
--  name,
--   STRING_AGG(DISTINCT CAST(EXTRACT(YEAR FROM date_column) AS STRING) ORDER BY CAST(EXTRACT(YEAR FROM date_column) AS STRING)) AS years_active,
--   COUNT(DISTINCT EXTRACT(YEAR FROM date_column)) AS num_years_active
-- FROM
--   {{ref('all_appointments')}}
-- GROUP BY
--   1
-- ORDER BY
--   1


-- first create a cleaned name column in the first CTE

with cleaned_names as(
SELECT
 TRIM(REPLACE(name, '\u0000', '')) AS name_cleaned,
 *
 FROM
  {{ref('all_appointments')}}
  )

  SELECT
  name_cleaned as name,
  STRING_AGG(DISTINCT CAST(EXTRACT(YEAR FROM date_column) AS STRING) ORDER BY CAST(EXTRACT(YEAR FROM date_column) AS STRING)) AS years_active,
  COUNT(DISTINCT EXTRACT(YEAR FROM date_column)) AS num_years_active,
  min(EXTRACT(YEAR FROM date_column)) as first_year_active,
  max(EXTRACT(YEAR FROM date_column)) as last_year_active
  FROM
  cleaned_names
GROUP BY
  1
ORDER BY
  1