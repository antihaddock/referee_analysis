
{{ config(materialized='table') }}


    with grades_renamed as (
        select
            app.primary_key
            , app.name
            , app.match_date
            , case
                when app.position = 'Assistant Referee 1' THEN 'Assistant Referee' 
                when app.position = 'Assistant Referee 2' THEN 'Assistant Referee' 
                WHEN REGEXP_CONTAINS(LOWER(app.position), r'(?i)line') THEN 'Assistant Referee'
                WHEN REGEXP_CONTAINS(LOWER(app.position), r'(?i)centre') THEN 'Referee' 
                WHEN REGEXP_CONTAINS(LOWER(app.position), r'(?i)4th') THEN 'Fourth Official' 
                else app.position 
            end as position
            , match_grade
            , kick_off
            , ground
            , home_team
            , away_team 
            , gr.competition
            , gr.grade
            , gr.senior_comp
            , EXTRACT(YEAR FROM match_date) as year
        from    
            {{ref('all_appointments')}} app
        inner join 
            {{ref('grades_renamed_dim')}} gr
            on gr.matchgrade = app.match_grade
    )

    , trial_match_flags as (
    select 
        t1.*,
        CASE    
                -- data from 2013 back does not include trial matches as they weren't billed
                when t1.match_date < '2014-01-01'
                then FALSE
                -- find of the match date is in the season
                WHEN t1.match_date BETWEEN t2.start_date AND t2.end_date
                THEN FALSE
                WHEN t1.match_date BETWEEN t2.start_date AND t2.end_date
                    -- in case the current season is running
                    AND t2.end_date is not null
                    THEN FALSE
                WHEN t1.grade is null
                    THEN null       
                WHEN t1.grade in ('Australia Cup', 'State Cup')
                    THEN FALSE    
                ELSE TRUE
            END AS trial_match_flag
        from 
            grades_renamed  as t1
        left join
            {{ref('transform_competitions')}} as t2
            on t1.year = t2.year 
            and t1.competition = t2.competition
    )

    select
        primary_key
        , name
        , match_date
        , position
        , kick_off
        , ground
        , home_team
        , away_team
        , competition
        , grade
        , year
        , trial_match_flag
        , senior_comp
    from 
        trial_match_flags


