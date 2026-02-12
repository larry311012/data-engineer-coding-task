
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select campaign_date
from "warehouse"."staging_intermediate"."int_campaign_daily"
where campaign_date is null



  
  
      
    ) dbt_internal_test