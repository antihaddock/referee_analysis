{{ config(materialized='table') }}

with cleaned_grades as(
SELECT
    *
 FROM
  {{ref('all_appointments')}}
  )

  select
  distinct(SPLIT(match_grade, ';')[OFFSET(0)]) AS match 
  -- 2015 matches have ; and then then grade after them which needs to be accounted for
  from
  cleaned_grades
  order by 1