{{ config(materialized='view') }}


SELECT * FROM {{ source('staging', 'external_fhv_tripdata_2019_non_partitioned')}}