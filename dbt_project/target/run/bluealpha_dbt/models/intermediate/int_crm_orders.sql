
      
        
        
        delete from "warehouse"."staging_intermediate"."int_crm_orders" as DBT_INTERNAL_DEST
        where (order_id) in (
            select distinct order_id
            from "int_crm_orders__dbt_tmp002429846201" as DBT_INTERNAL_SOURCE
        );

    

    insert into "warehouse"."staging_intermediate"."int_crm_orders" ("order_id", "customer_id", "order_date", "revenue", "channel_attributed", "campaign_source", "product_category", "region", "is_outlier", "is_refund", "load_ts")
    (
        select "order_id", "customer_id", "order_date", "revenue", "channel_attributed", "campaign_source", "product_category", "region", "is_outlier", "is_refund", "load_ts"
        from "int_crm_orders__dbt_tmp002429846201"
    )
  