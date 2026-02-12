
      
        delete from "warehouse"."staging_intermediate"."int_campaign_daily" as DBT_INTERNAL_DEST
        where (platform, campaign_id, campaign_date) in (
            select distinct platform, campaign_id, campaign_date
            from "int_campaign_daily__dbt_tmp002429852162" as DBT_INTERNAL_SOURCE
        );

    

    insert into "warehouse"."staging_intermediate"."int_campaign_daily" ("platform", "campaign_id", "campaign_name", "campaign_type", "campaign_date", "impressions", "clicks", "spend", "purchases", "purchase_value", "reach", "frequency", "load_ts", "ctr_pct", "cost_per_click", "cost_per_purchase")
    (
        select "platform", "campaign_id", "campaign_name", "campaign_type", "campaign_date", "impressions", "clicks", "spend", "purchases", "purchase_value", "reach", "frequency", "load_ts", "ctr_pct", "cost_per_click", "cost_per_purchase"
        from "int_campaign_daily__dbt_tmp002429852162"
    )
  