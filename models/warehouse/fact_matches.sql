{{
    config(
        partition_by={
            "field": "league_id",
            "data_type": "int64",
            "range": {"start": 0, "end": 10000, "interval": 1},
        }
    )
}}

with
    t1 as (
        select
            shots_on_goal,
            shots_off_goal,
            total_shots,
            blocked_shots,
            shots_insidebox,
            shots_outsidebox,
            fouls,
            corner_kicks,
            offsides,
            yellow_cards,
            red_cards,
            goalkeeper_saves,
            total_passes,
            passes_accurate,
            ball_possession_percentage,
            passes_percentage,
            t.team_id,
            f.match_id,
            round,
            league_id,
            name,
            short_name,
            country,
            city,
            state,
            cast(reference_date as date) as reference_date,
            cast(reference_date as time) as reference_time
        from {{ ref("stg_statistics") }} st
        left join {{ ref("stg_teams") }} t on t.team_id = st.team_id
        left join {{ ref("stg_fixtures") }} f on f.match_id = st.match_id
    )

select *
from t1

