with
    t1 as (
        select
            partida_id as match_id,
            datetime(
                parse_timestamp("%d/%m/%Y %H:%M", date), "America/Sao_Paulo"
            ) as reference_date,
            replace(rodada, 'Regular Season - ', '') as round,
            league_id,
            id_team_away,
            id_team_home
        from {{ source("cartola_tbl", "fixtures") }}
    )

select *
from t1

