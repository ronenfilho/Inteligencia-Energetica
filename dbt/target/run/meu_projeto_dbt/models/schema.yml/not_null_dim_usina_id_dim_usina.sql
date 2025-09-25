select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_dim_usina
from IE_DB.CORE.dim_usina
where id_dim_usina is null



      
    ) dbt_internal_test