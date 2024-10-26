-- Shows the proportion of each vehicle type in the dataset.
select
    "type",
    count(distinct track_id) * 1.0 / (select count(distinct track_id) from{{ source("fact_df_table",'fact_df') }}) as proportion
from
        {{ source("fact_df_table",'fact_df') }}
group by
    "type"
