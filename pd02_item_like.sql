-------------------------------------------------------- [1] item_like
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_item_like01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_item_like01 as
select
  a.f_ana01,
  a.order_user_id,
  date_diff(a.ymd_nn_31days, a.ymd_nn, day) as date_diff,
  count(distinct b.item_id) as cnt_like_item,
  sum(case when b.cnt_like is null then 0 else b.cnt_like end) as cnt_like_total
from
  fril-analysis-staging-165612.ueno_analysis02. pd01_ana01 as a
left join
  (select * from fril-analysis-staging-165612.ueno_analysis02.pd01_item_like01 where f_like = 1) as b 
                                on a.order_user_id = b.user_id and a.ymd_nn <= b.min_ts	and b.min_ts <= a.ymd_nn_31days
group by 1,2,3
order by 1,2,3
;


--- chk
select
  f_ana01,
  avg(date_diff),
  avg(cnt_like_item),
  avg(cnt_like_total)
from
  fril-analysis-staging-165612.ueno_analysis02.pd02_item_like01
group by 1
;
