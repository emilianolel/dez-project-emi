{{
    config(
        materialized='view'
    )
}}

with 
source as (

    select 
        cast(entity_code as STRING FORMAT '00') as entity_code,
        entity_name,
        cast(municipality_code as STRING FORMAT '00000') as municipality_code,
        municipality_name,
        affected_legal_asset,
        crime_type,
        crime_subtype,
        crime_modality_type,
        month as month_name,
        crimes,
        info_month_date
    from {{ source('idm-raw', 'raw_municipal_crime_incidence') }}
    where info_month_date is not null

),

renamed as (

    select

        {{ dbt_utils.generate_surrogate_key([ 'entity_code', 'municipality_code', 'affected_legal_asset', 'crime_type', 'crime_subtype', 'crime_modality_type', 'info_month_date']) }} as crime_key,
        entity_code,
        entity_name,
        ltrim(municipality_code) as municipality_code,
        municipality_name,
        affected_legal_asset,
        crime_type,
        crime_subtype,
        crime_modality_type,
        month_name,
        {{ get_month_number("month_name") }} as month_nbr,
        crimes,
        info_month_date

    from source

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}