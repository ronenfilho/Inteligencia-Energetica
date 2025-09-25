select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_dim_tempo
from IE_DB.CORE.dim_tempo
where id_dim_tempo is null



      
    ) dbt_internal_test