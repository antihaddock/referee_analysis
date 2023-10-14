{{ config(materialized='table') }}

-- first create a cleaned name column in the first CTE

with cleaned_names as(
SELECT
  *
 FROM
  {{ref('all_appointments')}}
  )

  SELECT
  name_cleaned as name,
  STRING_AGG(DISTINCT CAST(EXTRACT(YEAR FROM match_date) AS STRING) ORDER BY CAST(EXTRACT(YEAR FROM match_date) AS STRING)) AS years_active,
  COUNT(DISTINCT EXTRACT(YEAR FROM match_date)) AS num_years_active,
  min(EXTRACT(YEAR FROM match_date)) as first_year_active,
  max(EXTRACT(YEAR FROM match_date)) as last_year_active
  FROM
  cleaned_names
GROUP BY
  1
ORDER BY
  1