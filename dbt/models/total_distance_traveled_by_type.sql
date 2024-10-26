-- Sums the total distance traveled by each vehicle type.
select
    "type",
    sum(traveled_d) as total_distance_m
from
    {{ source("fact_df_table",'fact_df') }}
group by
    "type"
