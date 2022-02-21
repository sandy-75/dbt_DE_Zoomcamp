{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'fhv' as service_type 
    from {{ ref('staging_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.dispatching_base_num, 
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime,
    fhv_data.PULocationID,
    fhv_data.DOLocationID,
    fhv_data.SR_Flag,
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PULocationID = pickup_zone.locationid
