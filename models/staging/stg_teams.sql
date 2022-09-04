with
    t1 as (
        select
            cast(team_id as int) as team_id,
            name,
            code as short_name,
            country,
            trim(split(city, ',')[offset(0)]) as city,
            trim(array_reverse(split(city, ','))[offset(0)]) as state,
            logo,
            ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
        from {{ source("cartola_tbl", "teams") }}
    )

select team_id,
       name,
       short_name,
       country,
       city,
       state,
       logo
from t1
where row_num = 1

