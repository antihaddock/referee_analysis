
{{ config(materialized='table') }}

with union_all_apps as(
    select 
        name,
        date as matchdate,
        position,
        description as matchgrade,
        null as kickoff,
        null as ground,
        null as hometeam,
        null as awayteam
    from 
        {{source('raw','2009')}}
    union all

    select 
        name,
        date as matchdate,
        position,
        description as matchgrade,
        null as kickoff,
        null as ground,
        null as hometeam,
        null as awayteam
    from 
        {{source('raw','20102011')}}

    union all

    select 
        name,
        date as matchdate,
        position,
        description as matchgrade,
        null as kickoff,
        null as ground,
        null as hometeam,
        null as awayteam
    from 
        {{source('raw','2012')}}
    union all

    select 
        name,
        date as matchdate,
        position,
        description as matchgrade,
        null as kickoff,
        null as ground,
        null as hometeam,
        null as awayteam
    from 
        {{source('raw','20132014')}}

    union all

    select 
        name,
        date as matchdate,
        position,
        description as matchgrade,
        null as kickoff,
        null as ground,
        null as hometeam,
        null as awayteam
    from 
        {{source('raw','2015')}}

    union all

    select
        name,
        matchdate,
        position,
        matchgrade,
        kickoff,
        ground,
        hometeam,
        awayteam
    from
        {{source('raw','201620172018')}}

    union all

    select 
        name,
        position,   
        matchdate,
        matchgrade,
        kickoff,
        ground,
        hometeam,
        awayteam
    from
       {{source('raw','2019')}}

    union all

    select 
        name,
        position,
        matchdate,
        matchgrade,
        kickoff,
        ground,
        hometeam,
        awayteam
    from
        {{source('raw','2020')}}

    union all

    select 
        name,
        position,
        matchdate,
        matchgrade,
        kickoff,
        ground,
        hometeam,
        awayteam
    from
       {{source('raw','2021')}}

    union all

    select 
        name,
        position,
        matchdate,
        matchgrade,
        kickoff,
        ground,
        hometeam,
        awayteam
    from
      {{source('raw','2022')}}
)

select 
    INITCAP(name) as name,
    position,
    -- big long code block to coerce all the dates 
    CASE
      -- Check if the input date string matches the YYYY-MM-DD format
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', matchdate) IS NOT NULL THEN
        -- If it matches, convert it to the DD/MM/YYYY format
        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%Y-%m-%d', matchdate))
      -- Check if the input date string matches the MM/DD/YYYY format
      WHEN SAFE.PARSE_DATE('%m/%d/%Y', matchdate) IS NOT NULL THEN
        -- If it matches, convert it to the DD/MM/YYYY format
        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%m/%d/%Y', matchdate))
      -- Check if the input date string matches the DD/MM/YYYY format
      WHEN SAFE.PARSE_DATE('%d/%m/%Y', matchdate) IS NOT NULL THEN
        -- If it matches, leave it as is
        matchdate
      -- For all other cases, return NULL
      ELSE NULL
    END AS formatted_date,
    SAFE.PARSE_DATE('%d/%m/%Y', 
                    CASE
                      -- Check if the input date string matches the YYYY-MM-DD format
                      WHEN SAFE.PARSE_DATE('%Y-%m-%d', matchdate) IS NOT NULL THEN
                        -- If it matches, convert it to the DD/MM/YYYY format
                        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%Y-%m-%d', matchdate))
                      -- Check if the input date string matches the MM/DD/YYYY format
                      WHEN SAFE.PARSE_DATE('%m/%d/%Y', matchdate) IS NOT NULL THEN
                        -- If it matches, convert it to the DD/MM/YYYY format
                        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%m/%d/%Y', matchdate))
                      -- Check if the input date string matches the DD/MM/YYYY format
                      WHEN SAFE.PARSE_DATE('%d/%m/%Y', matchdate) IS NOT NULL THEN
                        -- If it matches, leave it as is
                        matchdate
                      -- For all other cases, return NULL
                      ELSE NULL
                    END) AS date_column,
    matchgrade,
    kickoff,
    ground,
    hometeam,
    awayteam
from 
    union_all_apps