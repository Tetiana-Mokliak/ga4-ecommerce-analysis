-- GA4 E-commerce Analysis
-- Dataset: ga4_obfuscated_sample_ecommerce
-- Tool: BigQuery
--
-- Project includes:
-- 1. ABC analysis
-- 2. Basket analysis
-- 3. Product funnel analysis
--
-- Author: Tetiana Mokliak

--=====================================================================================
--1. ABC-аналіз товарного асортимента – ранжування товарів по вкладу в загальну виручку 
--=====================================================================================

with revenue_table as(
  select
  i.item_name,
  i.item_category,
  sum(i.price * i.quantity) as revenue
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as ga
cross join unnest(ga.items) as i
where ga.event_name = 'purchase'
  and _TABLE_SUFFIX between '20210101' and '20210131'
group by i.item_name, item_category
),
cumulative_table as(
  select 
    item_name,
    item_category, 
    revenue,
    sum(revenue) over(order by revenue desc) as running_revenue,   -- cumulative revenue ordered by highest revenue
    sum(revenue) over () total_revenue,                            -- total revenue
    revenue / sum(revenue) over () * 100 as revenue_share,
    sum(revenue) over(order by revenue desc)/sum(revenue) over ()*100 as cumul_share,
    dense_rank() over (order by revenue desc) as rank_number
  from revenue_table
)
select
  rank_number,
  item_name,
  item_category,
  revenue,
  round(revenue_share,2) as revenue_share_pct,
  round(cumul_share, 2) as cumulative_share_pct,
  case when cumul_share<=80 then 'A'     -- products contributing to 80% of total revenue
       when cumul_share<=95 then 'B'     -- products contributing to 80% of total revenue
       else 'C'
  end as ABC_category,
  80 as threshold_80                     -- threshold line for visualization
from cumulative_table
order by revenue desc;

--==================================================================================
--ABC-аналіз товарного асортимента – групування товарів по вкладу в загальну виручку
--==================================================================================

with revenue_table as(
  select
  i.item_name,
  i.item_category,
  sum(i.price * i.quantity) as revenue
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as ga
cross join unnest(ga.items) as i
where ga.event_name = 'purchase'
  and _TABLE_SUFFIX between '20210101' and '20210131'
group by i.item_name, item_category
),
cumulative_table as(
  select 
    item_name,
    item_category, 
    revenue,
    sum(revenue) over(order by revenue desc) as running_revenue,
    sum(revenue) over () total_revenue,
    revenue / sum(revenue) over () * 100 as revenue_share,
    sum(revenue) over(order by revenue desc)/sum(revenue) over ()*100 as cumul_share,
    dense_rank() over (order by revenue desc) as rank_number
  from revenue_table
),
abc_table as (
  select
  rank_number,
  item_name,
  item_category,
  revenue,
  round(revenue_share,2) as revenue_share_pct,
  round(cumul_share, 2) as cumulative_share_pct,
  case when cumul_share<=80 then 'A'
       when cumul_share<=95 then 'B'
       else 'C'
  end as ABC_category,
  80 as threshold_80
from cumulative_table
order by revenue desc
)
select
  ABC_category,
  count(*) as item_count,
  sum(revenue) as total_revenue,
  round(sum(revenue) / sum(sum(revenue)) over () * 100, 2) as revenue_share_pct
from abc_table
group by ABC_category
order by ABC_category;

--===============================================
--2. Basket analysis. Які товари купують разом
--===============================================
with orders_table as(
  select
  concat(user_pseudo_id,event_timestamp) as order_id,
  i.item_name
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as ga
cross join unnest(ga.items) as i
where ga.event_name = 'purchase'
  and _TABLE_SUFFIX between '20210101' and '20210131'
),
paired_table as (
  select
    ot1.item_name as item_name1,
    ot2.item_name as item_name2
  from orders_table ot1
  join orders_table ot2 on ot1.order_id=ot2.order_id 
  and ot1.item_name<ot2.item_name)  -- generate unique product pairs without duplicates
select
item_name1,
item_name2,
count(*) as pairs_count
from paired_table
group by item_name1,item_name2
having count(*)>5
order by pairs_count desc;

--====================================================
--3. Product Funnel Analysis
--====================================================

with product_events as (
  select
    ga.user_pseudo_id,
    ga.event_name,
    i.item_name
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as ga
  cross join unnest(ga.items) as i
  where ga.event_name in ('view_item', 'add_to_cart', 'purchase')
    and i.item_name is not null                        -- remove missing product names
    and i.item_name != '(not set)'                     -- remove undefined placeholder values
    and _TABLE_SUFFIX between '20210101' and '20210131'
),
marked_table as (
  select
    user_pseudo_id,
    item_name,
    max(case when event_name = 'view_item' then 1 else 0 end) as viewed_flag,  -- create event flags
    max(case when event_name = 'add_to_cart' then 1 else 0 end) as added_flag,
    max(case when event_name = 'purchase' then 1 else 0 end) as purchased_flag
  from product_events
  group by user_pseudo_id, item_name
),
product_metrics as (
  select
    item_name,
    sum(viewed_flag) as view_users,  -- aggregate flagged users by product
    sum(added_flag) as cart_users,
    sum(purchased_flag) as purchase_users,
    round(safe_divide(sum(added_flag), sum(viewed_flag)) * 100, 2) as cart_conversion_pct,
    round(safe_divide(sum(purchased_flag), sum(viewed_flag)) * 100, 2) as purchase_from_view_pct,
    round(safe_divide(sum(purchased_flag), sum(added_flag)) * 100, 2) as purchase_from_cart_pct
  from marked_table
  group by item_name
)
select
  *,
  case
  when view_users >= 1000 and purchase_users = 0 then 'critical_issue'
  when view_users >= 1000 and purchase_from_view_pct >= 5 then 'strong_product'
  when view_users >= 1000 and purchase_from_view_pct < 2 then 'high_potential'
  when cart_users >= 100 and purchase_from_cart_pct < 20 then 'checkout_issue'
  when view_users < 200 and purchase_from_view_pct >= 5 then 'low_visibility'
  else 'other'
end as product_segment
from product_metrics
where view_users >= 10
order by purchase_users desc, view_users desc;












