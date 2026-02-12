
    
    



select campaign_id
from (select * from "warehouse"."staging_staging"."stg_google_ads_daily" where is_valid = true) dbt_subquery
where campaign_id is null


