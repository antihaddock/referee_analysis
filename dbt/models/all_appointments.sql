
{% set non_match_ref_years = ['2009', '20102011','2012', '20132014', '2015'] %}
{% set match_ref_years = ['201620172018', '2019','2020', '2021', '2022', '2023'] %}


{{ config(materialized='table') }}

with union_all_apps as(
    -- pre matchref years wrangle data into a format the same as matchref
    {% for year in non_match_ref_years %}
        select 
            TRIM(REPLACE(name, '\u0000', '')) AS name,
            TRIM(REPLACE(date, '\u0000', '')) AS match_date,
            TRIM(REPLACE(position, '\u0000', '')) AS position,
            -- 2015 is special as it has the teams listed in the match grade separated by a ;
            {% if year == '2015' %}
              TRIM(REPLACE(SUBSTR(description, 1, INSTR(description, ';')), '\u0000', '')) AS match_grade,
            {% else %}
                TRIM(REPLACE(description, '\u0000', '')) AS match_grade,
            {% endif %}
            null as kick_off,
            null as ground,
            null as home_team,
            null as away_team
        from 
            {{source('raw', year)}}
        {% if not loop.last %}
         union all
        {% endif %}
    {% endfor %}
    
        union all
    -- join all matchref years data to non match ref
    -- the import from raw is adding all these random '\u0000' onto strings
    -- the replace removes all these
    {% for year in match_ref_years %}
        select 
            TRIM(REPLACE(name, '\u0000', '')) AS name,
            TRIM(REPLACE(matchdate, '\u0000', '')) AS match_date,
            TRIM(REPLACE(position, '\u0000', '')) AS position_raw,
            TRIM(REPLACE(matchgrade, '\u0000', ''), ' ') AS match_grade,
            kickoff as kick_off,
            TRIM(REPLACE(ground, '\u0000', ''), ' ') AS ground,
            TRIM(REPLACE(hometeam, '\u0000', ''), ' ') AS home_team,
            TRIM(REPLACE(awayteam, '\u0000', ''), ' ') AS away_team,
        from
          {{source('raw', year)}}
        {% if not loop.last %}
         union all
         {% endif %}
    {% endfor %}
)

, tidy_dates as (
select 
    INITCAP(name) as name,
    position,
    CASE
      -- Check if the input date string matches the YYYY-MM-DD format
      WHEN SAFE.PARSE_DATE('%Y-%m-%d', match_date) IS NOT NULL THEN
        -- If it matches, convert it to the DD/MM/YYYY format
        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%Y-%m-%d', match_date))
      -- Check if the input date string matches the MM/DD/YYYY format
      WHEN SAFE.PARSE_DATE('%m/%d/%Y', match_date) IS NOT NULL THEN
        -- If it matches, convert it to the DD/MM/YYYY format
        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%m/%d/%Y', match_date))
      -- Check if the input date string matches the DD/MM/YYYY format
      WHEN SAFE.PARSE_DATE('%d/%m/%Y', match_date) IS NOT NULL THEN
        -- If it matches, convert it to the DD/MM/YYYY format
        FORMAT_DATE('%d/%m/%Y', SAFE.PARSE_DATE('%d/%m/%Y', match_date))
      -- For all other cases, return NULL
      ELSE NULL
    END AS cleaned_date,
    match_grade,
    kick_off,
    ground,
    home_team,
    away_team
from 
    union_all_apps
)

      select 
            {{ dbt_utils.generate_surrogate_key(['name', 'position', 'cleaned_date', 'match_grade', 'kick_off', 'ground', 'away_team', 'home_team']) }} as primary_key,
            name,
            SAFE.PARSE_DATE('%d/%m/%Y', cleaned_date) as match_date,
            position,
            match_grade,
            kick_off,
            ground,
            home_team,
            away_team 
      from
            tidy_dates
