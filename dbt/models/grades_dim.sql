{{ config(materialized='table') }}

with cleaned_grades as(
SELECT
-- Big Query is doing something odd with the names adding extra characters 
-- need to account for this by trimming and removing them
 TRIM(REPLACE(matchgrade, '\u0000', '')) AS grade_cleaned,
 *
 FROM
  {{ref('all_appointments')}}
  )

  select
  distinct(SPLIT(matchgrade, ';')[OFFSET(0)]) AS match 
  -- 2015 matches have ; and then then grade after them which needs to be accounted for
  from
  cleaned_grades
  order by 1