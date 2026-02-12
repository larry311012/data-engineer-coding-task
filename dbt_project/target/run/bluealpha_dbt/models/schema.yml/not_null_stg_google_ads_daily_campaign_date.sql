
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select campaign_date
from (select * from "warehouse"."staging_staging"."stg_google_ads_daily" where is_valid = true) dbt_subquery
where campaign_date is null



  
  
      
    ) dbt_internal_test