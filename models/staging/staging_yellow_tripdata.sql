{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata_partitioned') }}
  where vendorid is not null 
)
SELECT 
    -- identifiers
    {{ dbt_utils.surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
    VendorID,
    RatecodeID,
    PULocationID as pickup_locationid,
    DOLocationID as dropoff_locationid,
    
    -- timestamps
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    passenger_count as passenger_count,
    trip_distance as trip_distance,
    
    -- payment info
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type, 
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    congestion_surcharge    
FROM tripdata
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  LIMIT 100

{% endif %}