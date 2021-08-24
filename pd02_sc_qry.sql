-------------------------------------------------------- [1] Search queries
------ [1-1] analysis base
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry_pt01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry_pt01 as
select
  a.f_ana01,
  a.order_user_id,
  b.query,	
  b.active,	
  b.notification_frequency,
  b.merged_search_query_id,
  b.created_at,
  b.updated_at,
  b.txt,
  b.status,
  b.brand,
  b.parent_category,
  b.category,
  b.color,
  b.size,
  b.size_group,
  b.min_sell_price,
  b.max_sell_price,
  b.tran_status
from
  fril-analysis-staging-165612.ueno_analysis02. pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_sc_qry01 as b 
          on a.order_user_id = b.user_id and a.ymd_nn <= b.created_at	and b.created_at <= a.ymd_nn_31days
;

--- chk
select 
  f_ana01,
  case when created_at is not null then 1 else 0 end as f_sq,
  count(distinct order_user_id)
from
  fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry_pt01
group by rollup(1,2)
order by 1,2
;


------ [1-2] Search queries 
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry01 as
select
  f_ana01,
  order_user_id,
  max(case when active is not null then 1 else 0 end) as f_act,
  --avg(case when notification_frequency is not null then notification_frequency else 0 end) as f_notification_frq,
  max(case when txt is not null then 1 else 0 end) as f_txt,
  max(case when status is not null then 1 else 0 end) as f_status,
  max(case when brand is not null then 1 else 0 end) as f_brand,
  max(case when parent_category is not null then 1 else 0 end) as f_parent_category,
  max(case when category is not null then 1 else 0 end) as f_category,
  max(case when color is not null then 1 else 0 end) as f_color,
  max(case when size is not null then 1 else 0 end) as f_size,
  max(case when size_group is not null then 1 else 0 end) as f_size_group,
  max(case when min_sell_price is not null then 1 else 0 end) as f_min_sell_price,
  max(case when max_sell_price is not null then 1 else 0 end) as f_max_sell_price,
  max(case when tran_status is not null then 1 else 0 end) as f_tran_status
from
  fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry_pt01
group by 1,2
order by 1,2
;


--- chk
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry_pt01 where order_user_id in (941,2550,2672);
select f_ana01,f_notification_frq, count(distinct order_user_id), count(*) from fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry01 group by 1,2 order by 1,2;
select  notification_frequency, count(distinct user_id), count(*) from fril-analysis-staging-165612.ueno_analysis02.pd01_sc_qry01 group by 1,2 order by 1,2;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry01 where f_notification_frq > 0 limit 100;
