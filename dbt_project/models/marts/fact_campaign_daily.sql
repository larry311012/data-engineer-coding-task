{{config(materialized='table')}}

with campaigns as (
    select * from {{ ref('int_campaign_daily')}}
),

crm_daily as (
    select
        campaign_source as campaign_id,
        order_date as campaign_date,
        count(*) as order_count,
        sum(case when is_refund=FALSE then revenue else 0 end) as attributed_revenue,
        sum(case when is_refund=TRUE then revenue else 0 end) as refund_amount,
        count(distinct customer_id) as unique_customers
    from {{ref('int_crm_orders')}}
    where campaign_source is not null
    group by campaign_source, order_date
)

select 
    c.platform,
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.campaign_date,
    c.impressions,
    c.clicks,
    c.spend,
    c.purchases,
    c.purchase_value,
    c.reach,
    c.frequency,
    c.ctr_pct,
    c.cost_per_click,
    coalesce(r.order_count,0) as crm_order_count,
    coalesce(r.attributed_revenue,0) as crm_attributed_revenue,
    coalesce(r.refund_amount,0) as crm_refund_amount,
    coalesce(r.unique_customers,0) as crm_unique_customers,
    case 
        when spend>0 then round(coalesce(r.attributed_revenue,0) / c.spend, 2)
        else 0
    end as return_of_ads_spend
from campaigns c
left join crm_daily r
    on c.campaign_id = r.campaign_id
    and c.campaign_date = r.campaign_date
