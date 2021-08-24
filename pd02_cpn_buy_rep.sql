
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01 as
select
  a.order_user_id,
  a.f_ana01,
  a.order_request_id_exist,
  b.coupon_discount_price,
  case 
    when b.coupon_discount_price is null  then '01_leave' 
    when b.coupon_discount_price = 0      then '02_not_cpn'
    when b.coupon_discount_price > 0      then '03_cpn'
    else '99_uk'
  end as f_not_cpn
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  (select * from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01_1) as b on a.order_request_id_exist = b.order_request_id
order by 1,2
;


---- CHK
select
  f_ana01,
  f_not_cpn,
  count(distinct order_user_id) as uu,
  count(*) as cnt
from
  fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01
group by rollup(1,2)
order by 1,2
;


select * from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01 limit 100;