{{
    config(
        materialized='table',
        partition_by={"field": "info_month_date", "data_type": "date"},
         cluster_by = ["municipality_full_code"]
    )
}}


with mexico_crimes as (
    
    select 
        *
    from 
        {{ ref('stg_idm-raw__raw_municipal_crime_incidence') }}
    
),

mexico_coords as (

    select
        *
    from
        {{ ref('stg_coords-raw__mexico_coordinates') }}

)

select 
    mcr.crime_key
    , mco.entity_code
    , mco.entity_name
    , mco.entity_name_short
    , mco.municipality_code
    , mco.municipality_full_code
    , mco.municipality_name
    , mco.latitude
    , mco.longitude
    , mco.altitude
    , mcr.affected_legal_asset
    , mcr.crime_type
    , mcr.crime_subtype
    , mcr.crime_modality_type
    , mcr.month_name
    , mcr.month_nbr
    , mcr.crimes
    , mcr.info_month_date
from 
    mexico_crimes as mcr
inner join 
    mexico_coords as mco
on
    mcr.municipality_code = mco.municipality_full_code

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}