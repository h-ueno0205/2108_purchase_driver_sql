------- [1-1] Rakuten cash / Rakuten point
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01 as
select 
  a.f_ana01,
  a.order_user_id,
  a.order_request_id_nn,
  a.sell_price_nn,
  a.coupon_discount_price,
  a.use_point_num,
  a.use_balance_num,
  b.point,
  case 
    when b.point is null then 0 
    else (a.coupon_discount_price + a.use_point_num + a.use_balance_num + b.point) / a.sell_price_nn 
  end as r_point,
  case 
    when b.point is null then 0 
    when (a.coupon_discount_price + a.use_point_num + a.use_balance_num + b.point) / a.sell_price_nn = 1 then 1
    else 0
  end as f_point
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_rsp_rc01 as b on a.order_request_id_nn = b.order_request_id
order by 1,2
;



----
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01 where f_point = 1 and f_ana01 = '09_leave' limit 100;

select count(*), count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc_pt01;

select 
  f_ana01,
  case
    when r_point = 0 then -1
    else round(r_point*10,0)*10
  end as f_r_point,
  count(distinct order_user_id) as uu,
  avg(sell_price_nn) as avg_sp
from 
  fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01 
group by rollup(1,2)
order by 1,2
;


select 
  f_ana01,
  round(r_point*10,0)*10,
  count(distinct order_user_id) as uu,
  avg(sell_price_nn) as avg_sp
from 
  fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01 
group by rollup(1,2)
order by 1,2
;

