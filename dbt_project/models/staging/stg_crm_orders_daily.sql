{{ config(materialized='view')}}

with src as (
    select * from raw.crm_revenue_raw
),
--  dedupe by order_id, keep first row
deduped as (
    select *,
        row_number() over (partition by order_id order by row_num) as rn
    from src
),
clean as (
    select
        row_num,
        nullif(trim(order_id),'') as order_id,
        nullif(trim(customer_id),'') as customer_id,
        CASE  
            when order_date ~'^\d{4}-\d{2}-\d{2}$' then to_date(order_date,'YYYY-MM-DD')
            when order_date ~'^\d{2}/\d{2}/\d{4}$' and split_part(order_date,'/',1)::int > 12 then to_date(order_date,'DD/MM/YYYY')
            when order_date ~'^\d{2}/\d{2}/\d{4}$' then to_date(order_date,'MM/DD/YYYY')
            when order_date ~'^\w+ \d{1,2}, \d{4}$' then to_date(order_date,'Month DD, YYYY')
            else null
        end as order_date,
        nullif(revenue,'')::numeric as revenue,
        lower(nullif(trim(channel_attributed),'')) as channel_attributed,
        nullif(trim(campaign_source),'') as campaign_source,
        nullif(trim(product_category),'') as product_category,
        nullif(trim(region),'') as region,
        run_id,
        load_ts::timestamp as load_ts,
        load_date::date as load_date,
        -- take care of outlier
        case 
            when nullif(revenue, '')::numeric > 10000 then TRUE
            else FALSE
        end as is_outlier,
        case 
            when nullif(revenue, '')::numeric < 0 then TRUE
            else FALSE
        end as is_refund
    from deduped
    where rn =1 
),
validate as (
    select *,
        (
            order_id is not null 
            and customer_id is not null 
            and revenue is not null 
            and order_date is not null
        ) as is_valid
    from clean
)
select 
    'crm'::text as platform,
    *
from validate