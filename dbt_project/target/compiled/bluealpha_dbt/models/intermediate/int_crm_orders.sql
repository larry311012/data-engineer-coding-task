

select
    order_id,
    customer_id,
    order_date,
    revenue,
    channel_attributed,
    campaign_source,
    product_category,
    region,
    is_outlier,
    is_refund,
    load_ts
from "warehouse"."staging_staging"."stg_crm_orders_daily"
where 
    is_valid = TRUE
    and is_outlier = FALSE

    and load_ts > (select max(load_ts) from "warehouse"."staging_intermediate"."int_crm_orders")
