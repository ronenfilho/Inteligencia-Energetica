select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select instante
from LAB_PIPELINE.STAGING.stg_usina_disp
where instante is null



      
    ) dbt_internal_test