

with google as (
    select
        platform,
        campaign_id,
        campaign_name,
        campaign_type,
        campaign_date,
        impressions,
        clicks,
        spend,
        --asumption Google Ads "conversions" and Facebook "purchases" represent the same business event
        conversions as purchases,
        conversion_value as purchase_value,
        null::bigint as reach,
        null::numeric as frequency,
        is_valid,
        load_ts
    from "warehouse"."staging_staging"."stg_google_ads_daily"
    where is_valid = TRUE
),
facebook as (
    select
        platform,
        campaign_id,
        campaign_name,
        null::text as campaign_type,
        campaign_date,
        impressions,
        clicks,
        spend,
        purchases,
        purchase_value,
        reach,
        frequency,
        is_valid,
        load_ts
    from "warehouse"."staging_staging"."stg_facebook_ads_daily"
    where is_valid = TRUE
),
unioned as (
    select * from google
    union all
    select * from facebook
)
select 
    platform,
    campaign_id,
    campaign_name,
    campaign_type,
    campaign_date,
    impressions,
    clicks,
    spend,
    purchases,
    purchase_value,
    reach,
    frequency,
    load_ts,
    case when impressions > 0
        then round(clicks::numeric / impressions *100, 2)
        else 0
    end as ctr_pct,
    case when clicks > 0
        then round(spend / clicks, 2)
        else 0
    end as cost_per_click,
    case when purchases > 0
        then round(spend / purchases, 2)
        else 0
    end as cost_per_purchase
from unioned


where load_ts > (select max(load_ts) from "warehouse"."staging_intermediate"."int_campaign_daily")
