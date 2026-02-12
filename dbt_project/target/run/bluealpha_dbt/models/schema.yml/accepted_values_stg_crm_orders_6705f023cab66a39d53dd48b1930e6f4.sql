
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        channel_attributed as value_field,
        count(*) as n_records

    from (select * from "warehouse"."staging_staging"."stg_crm_orders_daily" where is_valid = true and channel_attributed is not null) dbt_subquery
    group by channel_attributed

)

select *
from all_values
where value_field not in (
    'google','facebook','email','direct','organic'
)



  
  
      
    ) dbt_internal_test