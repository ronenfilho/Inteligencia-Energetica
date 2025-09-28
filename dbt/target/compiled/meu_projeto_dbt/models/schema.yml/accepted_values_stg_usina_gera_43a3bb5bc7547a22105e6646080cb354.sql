
    
    

with all_values as (

    select
        data_type as value_field,
        count(*) as n_records

    from IE_DB.STAGING.stg_usina_geracao
    group by data_type

)

select *
from all_values
where value_field not in (
    'annual','monthly','fallback'
)


