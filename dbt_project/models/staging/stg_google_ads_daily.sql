{{ config(materialized='view')}}

with src as (
    select * from raw.google_ads_raw
),

clean as (
    select
        row_num,
        nullif(trim(campaign_id),'') as campaign_id,
        nullif(trim(campaign_name),'') as campaign_name,
        nullif(trim(campaign_type),'') as campaign_type,
        nullif(trim(status),'') as status,
        CASE  
            when date ~'^\d{4}-\d{2}-\d{2}$' then to_date(date,'YYYY-MM-DD')
            else null
        end as campaign_date,
        nullif(impressions,'')::bigint as impressions,
        nullif(clicks,'')::bigint as clicks,
        -- cost_micros -> spend
        (nullif(cost_micros,'')::numeric /1000000.0) as spend,
        nullif(conversions,'')::bigint as conversions,
        nullif(conversion_value,'')::numeric as conversion_value,
        run_id,
        load_ts::timestamp as load_ts,
        load_date::date as load_date
    from src
),
validate as (
    select 
        *,
        (campaign_id is not null and campaign_date is not null and spend is not null and spend>=0) as is_valid
    from clean
)

select 
    'google'::text as platform,
    *
from validate