-- Summarizes performance metrics (avg speed, total distance) by vehicle type.
select
    "type",
    count(distinct f.track_id) as vehicle_count,
    avg(avg_speed) as avg_speed_kmh,
    sum(traveled_d) as total_distance_m
from
    {{ source("fact_df_table",'fact_df') }} as f
inner join 
    {{ source("dimensional_df_table",'dimensional_df') }} as d
on 
    f.track_id = d.track_id
group by
    "type"
