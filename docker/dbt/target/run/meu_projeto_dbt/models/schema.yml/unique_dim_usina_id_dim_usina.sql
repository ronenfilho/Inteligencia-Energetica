select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    id_dim_usina as unique_field,
    count(*) as n_records

from LAB_PIPELINE.CORE.dim_usina
where id_dim_usina is not null
group by id_dim_usina
having count(*) > 1



      
    ) dbt_internal_test