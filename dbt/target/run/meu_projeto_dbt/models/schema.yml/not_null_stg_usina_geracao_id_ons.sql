select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_ons
from IE_DB.STAGING.stg_usina_geracao
where id_ons is null



      
    ) dbt_internal_test