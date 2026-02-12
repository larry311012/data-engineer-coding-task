
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from (select * from "warehouse"."staging_staging"."stg_crm_orders_daily" where is_valid = true) dbt_subquery
where order_id is null



  
  
      
    ) dbt_internal_test