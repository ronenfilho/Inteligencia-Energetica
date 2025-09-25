select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_dim_localidade
from LAB_PIPELINE.CORE.dim_localidade
where id_dim_localidade is null



      
    ) dbt_internal_test