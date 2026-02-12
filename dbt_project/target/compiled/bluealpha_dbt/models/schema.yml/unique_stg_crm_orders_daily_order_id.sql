
    
    

select
    order_id as unique_field,
    count(*) as n_records

from (select * from "warehouse"."staging_staging"."stg_crm_orders_daily" where is_valid = true) dbt_subquery
where order_id is not null
group by order_id
having count(*) > 1


