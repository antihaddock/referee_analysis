{{ config(materialized='table') }}
    
    select
        competition,
        year,
        PARSE_DATE('%d/%m/%Y', start_date) AS start_date,
        PARSE_DATE('%d/%m/%Y', end_date) AS end_date
    from
        {{ ref('Competitions_dim') }}
