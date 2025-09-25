select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select id_dim_tempo as from_field
    from IE_DB.CORE.fact_disponibilidade
    where id_dim_tempo is not null
),

parent as (
    select id_dim_tempo as to_field
    from IE_DB.CORE.dim_tempo
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test