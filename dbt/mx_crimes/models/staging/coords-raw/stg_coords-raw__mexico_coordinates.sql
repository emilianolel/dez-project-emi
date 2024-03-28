{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select 
        *
    from 
        {{ source('coords-raw', 'raw_mexico_coordinates') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['entity_code', 'entity_name', 'entity_name_short', 'municipality_code', 'municipality_name']) }} as municipality_key
        , entity_code
        , entity_name
        , entity_name_short
        , municipality_code
        , concat(entity_code, municipality_code) as municipality_full_code
        , municipality_name
        , AVG(latitude_decimal) as latitude
        , AVG(longitude_decimal) as longitude
        , AVG(altitude) as altitude
        , ST_GEOGPOINT(AVG(longitude_decimal), AVG(latitude_decimal)) as geo_point
        , SUM(total_population) as total_population
        , SUM(masculine_population) as masculine_population
        , SUM(feminine_population) as feminine_population
        , SUM(inhabited_homes) as inhabited_homes

    from 
        source
    group by 
        entity_code
        , entity_name
        , entity_name_short
        , municipality_code
        , municipality_name
)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}