---- [1-1] Item / ssn and view raw01
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_item_view01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_item_view01 as
select
  a.f_ana01,
  a.order_user_id,
  date_diff(a.ymd_nn_31days, a.ymd_nn, day) as date_diff,
  count(distinct b.ssn_no) as cnt_ssn,
  count(distinct b.item_id) as cnt_item,
  count(distinct case when b.web = true then ssn_no || web else null end) as cnt_ssn_web,
  sum(case when iv_sec is null then 0 else iv_sec/60 end) as ttl_dur_min,
  sum(case when leave_hour is null then 0 else  leave_hour end) as ttl_leave_hour
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_item_view02 as b on a.order_user_id = b.user_id and a.ymd_nn < b.cur_iv_utc and b.cur_iv_utc <= a.ymd_nn_31days
group by 1,2,3
order by 1,2,3
;
