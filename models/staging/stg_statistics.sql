{% set columns = ['Shots_on_Goal', 'Shots_off_Goal', 'Total_Shots', 'Blocked_Shots', 'Shots_insidebox', 'Shots_outsidebox', 'Fouls', 'Corner_Kicks', 'Offsides', 'Yellow_Cards', 'Red_Cards', 'Goalkeeper_Saves', 'Total_passes', 'Passes_accurate', 'team_id'] %}

with t1 as (
    select 
        {% for column in columns %}
            COALESCE({{ column }}, 0) as {{ column }}, 
        {% endfor %}
        CAST(REPLACE(Ball_Possession, '%', '') as int) as Ball_Possession_Percentage,
        CAST(REPLACE(Passes__, '%', '') as int) as Passes_Percentage,
        CAST(match_id as int) as  match_id
    
    from {{ source('cartola_tbl', 'statistics') }}
),
t2 as (
SELECT
  Shots_on_Goal,
  Shots_off_Goal,
  Total_Shots,
  Blocked_Shots,
  Shots_insidebox,
  Shots_outsidebox,
  Fouls,
  Corner_Kicks,
  Offsides,
  Yellow_Cards,
  Red_Cards,
  Goalkeeper_Saves,
  Total_passes,
  Passes_accurate,
  Ball_Possession_Percentage,
  Passes_Percentage,
   team_id,
  match_id
FROM
  t1
)


select * from t2