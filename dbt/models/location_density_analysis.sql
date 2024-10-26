-- Analyzes the density of vehicles by latitude and longitude.
select
    lat,
    lon,
    count(distinct f.track_id) as vehicle_count
from
    {{ source("fact_df_table",'fact_df') }} as f
inner join 
    {{ source("dimensional_df_table",'dimensional_df') }} as d
on 
    f.track_id = d.track_id
group by
    lat, lon
order by
    vehicle_count desc
