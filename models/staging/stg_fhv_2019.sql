{{ config(materialized='view') }}


select 
    dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    SR_Flag as sr_flag

from {{ source('staging','fhv_external') }}
-- where EXTRACT(YEAR FROM pickup_datetime) = 2019
-- dbt build -m stg_fhv_2019.sql --vars '{is_test_run:false}'

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}