-- Calculates average distance traveled for each vehicle type.
select
    "type",
    avg(traveled_d) as avg_distance_m
from
    {{ source("fact_df_table","fact_df") }}
group by
    "type"
