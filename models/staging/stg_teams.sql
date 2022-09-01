with
    t1 as (
        select
            cast(team_id as int) as team_id,
            name,
            code as short_name,
            country,
            trim(split(city, ',')[offset(0)]) as city,
            trim(array_reverse(split(city, ','))[offset(0)]) as state
        from {{ source("cartola_tbl", "teams") }}
    )

select *
from t1

