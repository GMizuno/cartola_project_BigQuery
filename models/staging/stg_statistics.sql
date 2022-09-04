{% set columns = [
    "Shots_on_Goal",
    "Shots_off_Goal",
    "Total_Shots",
    "Blocked_Shots",
    "Shots_insidebox",
    "Shots_outsidebox",
    "Fouls",
    "Corner_Kicks",
    "Offsides",
    "Yellow_Cards",
    "Red_Cards",
    "Goalkeeper_Saves",
    "Total_passes",
    "Passes_accurate",
    "team_id",
] %}

with
    t1 as (
        select
            {% for column in columns %}
            coalesce({{ column }}, 0) as {{ column }},
            {% endfor %}
            cast(
                replace(ball_possession, '%', '') as int
            ) as ball_possession_percentage,
            cast(replace(passes_percentage, '%', '') as int) as passes_percentage,
            cast(match_id as int) as match_id

        from {{ source("cartola_tbl", "statistics") }}
    ),
    t2 as (
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
            team_id,
            match_id,
            ROW_NUMBER() OVER (PARTITION BY team_id, match_id) AS row_num
        from t1
    )


select shots_on_goal,
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
            team_id,
            match_id
from t2
where row_num = 1

