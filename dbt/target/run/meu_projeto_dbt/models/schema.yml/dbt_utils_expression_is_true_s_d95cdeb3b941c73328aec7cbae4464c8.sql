select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from IE_DB.STAGING.stg_usina_geracao

where not(val_geracao_mw val_geracao_mw >= 0 OR val_geracao_mw IS NULL)


      
    ) dbt_internal_test