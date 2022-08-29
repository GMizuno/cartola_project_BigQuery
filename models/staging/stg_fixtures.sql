with t1 as (
    select 
       *
    from {{ source('cartola_tbl', 'fixtures') }}
)

select * from t1