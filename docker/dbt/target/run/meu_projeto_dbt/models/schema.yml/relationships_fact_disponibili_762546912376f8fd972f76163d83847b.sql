select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select id_dim_usina as from_field
    from LAB_PIPELINE.CORE.fact_disponibilidade
    where id_dim_usina is not null
),

parent as (
    select id_dim_usina as to_field
    from LAB_PIPELINE.CORE.dim_usina
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test