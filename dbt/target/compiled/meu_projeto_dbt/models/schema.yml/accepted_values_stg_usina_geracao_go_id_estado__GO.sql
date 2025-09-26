
    
    

with all_values as (

    select
        id_estado as value_field,
        count(*) as n_records

    from IE_DB.STAGING.stg_usina_geracao_go
    group by id_estado

)

select *
from all_values
where value_field not in (
    'GO'
)


