{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_fhv_2019') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    dispatching_base_num,
    pickup_datetime,
    EXTRACT(year FROM pickup_datetime) as pickup_year,
    EXTRACT(month FROM pickup_datetime) as pickup_month,
    dropoff_datetime,
    EXTRACT(year FROM dropoff_datetime) as dropoff_year,
    EXTRACT(month FROM dropoff_datetime) as dropoff_month,
    fhv_data.pickup_locationid,
    fhv_data.dropoff_locationid,
    sr_flag
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
