{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by CAST(vendor_id AS INT64), tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata_external') }}
  where vendor_id is not null 
)
select
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendor_id', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendor_id as integer) as vendor_id,
    cast(ratecode_id as integer) as ratecodeid,
    cast(pu_location_id as integer) as  pickup_locationid,
    cast(do_location_id as integer) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}