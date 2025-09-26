select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_table_name
from IE_DB.STAGING.stg_usina_geracao
where source_table_name is null



      
    ) dbt_internal_test