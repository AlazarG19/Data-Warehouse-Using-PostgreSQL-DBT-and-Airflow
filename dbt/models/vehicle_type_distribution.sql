-- distribution of vehicle types
select
    "type",
    count(distinct track_id) as vehicle_count
from
    {{ source("fact_df_table",'fact_df') }}
group by
    "type"
order by
    vehicle_count desc
