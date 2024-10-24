-- analyze average speed against distance travled
select
    f.track_id,
    traveled_d,
    avg_speed,
    (traveled_d / avg_speed) as time_taken_hours
from
    {{ source("fact_df_table",'fact_df') }} as f
inner join 
    {{ source("dimensional_df_table",'dimensional_df') }} as d
on 
    f.track_id = d.track_id
