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
            id_team_home,
            ROW_NUMBER() OVER (PARTITION BY partida_id) AS row_num
        from {{ source("cartola_tbl", "matches") }}
    )

select  match_id,
        reference_date,
        round,
        league_id,
        id_team_away,
        id_team_home
from t1
where row_num = 1

