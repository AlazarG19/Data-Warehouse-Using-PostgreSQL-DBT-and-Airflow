-- calculate the average speed of each vehicle type
select
    "type",
    avg(avg_speed) as avg_speed_kmh
from
    {{source("fact_df_table",'fact_df')  }}
group by
    "type"
