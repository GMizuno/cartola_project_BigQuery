with t1 as (
    select 
        CAST(team_id as int) as team_id,
        name,
        code as short_name,
        country,
        TRIM(SPLIT(city, ',')[OFFSET(0)]) AS city,
        TRIM(ARRAY_REVERSE(SPLIT(city, ','))[OFFSET(0)]) as state
    from {{ source('cartola_tbl', 'teams') }}
)

select * from t1