
    
    

with child as (
    select id_dim_localidade as from_field
    from IE_DB.CORE.fact_disponibilidade
    where id_dim_localidade is not null
),

parent as (
    select id_dim_localidade as to_field
    from IE_DB.CORE.dim_localidade
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


