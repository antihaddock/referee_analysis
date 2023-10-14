{{ config(materialized='table') }}

    select
        name
        , match_date
        , position
        , kick_off
        , ground
        , home_team
        , away_team
        , competition
        , grade
    from
        {{ ref('cleaned_appointments') }}
    where
        trial_match_flag = FALSE
        and senior_comp = TRUE
        and competition !='Juniors'