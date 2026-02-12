
  create view "warehouse"."staging_staging"."stg_facebook_ads_daily__dbt_tmp"
    
    
  as (
    

with src as (
    select * from raw.facebook_export_raw
),

clean as (
    select
        row_num,
        nullif(trim(campaign_id),'') as campaign_id,
        nullif(trim(campaign_name),'') as campaign_name,
        CASE  
            when date ~'^\d{4}-\d{2}-\d{2}$' then to_date(date,'YYYY-MM-DD')
            when date ~'^\d{2}/\d{2}/\d{4}$' then to_date(date,'MM/DD/YYYY')
            when date ~'^\d{4}/\d{2}/\d{2}$' then to_date(date,'YYYY/MM/DD')
            when date ~'^\d{2}-\d{2}-\d{4}$' then to_date(date,'MM-DD-YYYY')
            when date ~'^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(date,'DD-Mon-YYYY')
            else null
        end as campaign_date,
        nullif(impressions,'')::bigint as impressions,
        nullif(clicks,'')::bigint as clicks,
        nullif(spend,'')::numeric as spend,
        coalesce(nullif(purchases,'')::bigint,0) as purchases,
        coalesce(nullif(purchase_value,'')::numeric,0) as purchase_value,
        nullif(reach,'')::bigint as reach,
        nullif(frequency,'')::numeric as frequency,
        run_id,
        load_ts::timestamp as load_ts,
        load_date::date as load_date
    from src
),
validate as (
    select 
        *,
        (
            campaign_id is not null 
            and campaign_date is not null 
            and spend is not null 
            and spend>=0
        ) as is_valid
    from clean
)
select 
    'facebook'::text as platform,
    *
from validate
  );