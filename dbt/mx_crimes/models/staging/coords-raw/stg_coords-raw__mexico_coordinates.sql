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
        {{ source('coords-raw', 'mexico_coordinates') }}

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
        , AVG(latitude) as latitude
        , AVG(longitude) as longitude
        , AVG(altitude) as altitude

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