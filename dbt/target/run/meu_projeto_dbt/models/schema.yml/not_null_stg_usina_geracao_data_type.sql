select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select data_type
from IE_DB.STAGING.stg_usina_geracao
where data_type is null



      
    ) dbt_internal_test