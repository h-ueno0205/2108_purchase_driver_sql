-------------------------------------------------------- [1] balance
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_blc01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_blc01 as
select
  a.f_ana01,
  a.order_user_id,
  sum(case when b.f_blc_sign * amount is not null and b.f_blc_6m in ('01_6m', '02_before')  then b.f_blc_sign * amount else 0 end) as saving,
  sum(case when b.f_blc_type = '02_sales'         and b.f_blc_6m in ('01_6m')               then b.amount else 0 end) as sales,
  sum(case when b.f_blc_type = '05_r_cash'        and b.f_blc_6m in ('01_6m')               then b.amount else 0 end) as to_r_cash,
  sum(case when b.f_blc_type = '01_out_rakuten'   and b.f_blc_6m in ('01_6m')               then b.amount else 0 end) as out_rtn,
  sum(case when b.f_blc_type = '04_delivery'      and b.f_blc_6m in ('01_6m')               then b.amount else 0 end) as delivery
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  (select * from fril-analysis-staging-165612.ueno_analysis02.pd01_balance03 where f_blc_6m in ('01_6m', '02_before')) as b on a.order_user_id = b.order_user_id
group by 1,2
order by 1,2
;

--- chk
select distinct f_blc_type from fril-analysis-staging-165612.ueno_analysis02.pd01_balance03;

-------------------------------------------------------- [2] Coupon
--- [2-1] Coupon use at New user
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_use01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_use01 as
select
  a.f_ana01,
  a.order_user_id,
  a.order_request_id_nn,
  a.coupon_discount_price,
  a.use_point_num,
  a.use_balance_num,
  b.discount_price_mst,
  b.discount_rate_mst,
  b.passive,
  b.max_price,
  b.min_price,
  
  --- flagged
  case when a.coupon_discount_price > 0 then 1 else 0 end as f_cpn_discount,
  case when a.use_point_num > 0 then 1 else 0 end as f_buy_point,
  case when a.use_balance_num > 0 then 1 else 0 end as f_buy_blc,
  case when b.discount_price_mst > 0 then 1 else 0 end as f_cpn_act,
  case when b.discount_rate_mst > 0 then 1 else 0 end as f_cpn_rate,
  case when b.passive > 0 then 1 else 0 end as f_cpn_passive,
  case when b.max_price > 0 then 1 else 0 end as f_cpn_max_pr,
  case when b.min_price > 0 then 1 else 0 end as f_cpn_min_pr,
 
  case when b.boost_flag > 0 then 1 else 0 end as f_cpn_bst,
  case when b.buy_new > 0 then 1 else 0 end as f_cpn_buy_new,
  case when b.buy_existing > 0 then 1 else 0 end as f_cpn_buy_ext,
  case when b.buy_other > 0 then 1 else 0 end as f_cpn_buy_oth,

  case when b.buy_return > 0 then 1 else 0 end as f_cpn_buy_ret,
  case when b.sell_new > 0 then 1 else 0 end as f_cpn_sell_new,
  case when b.sell_return > 0 then 1 else 0 end as f_cpn_sell_ret,
  case when b.sell_other > 0 then 1 else 0 end as f_cpn_sell_oth
  
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_cpn_users01 as b on a.order_request_id_nn = b.order_request_id
;

--- [2-1] Coupon delivery after first buy
--[2-1-1] including first buy
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01 as
select
  a.f_ana01,
  a.order_user_id,
  a.order_request_id_nn,
  a.order_request_id_exist,
  a.ymd_nn,
  a.ymd_nn_31days,
  a.coupon_discount_price,
  a.ordered_at_nn,
  a.ordered_at_nn_31days,
 
  -- distribute coupons
  b.cpn_dst_id,
  b.user_id,
  b.coupon_id,
  b.name,
  b.available_start_at_act,
  b.available_end_at_act,
  b.expired_term_mst,
  b.discount_price_mst,
  b.discount_rate_mst,
  b.passive	,
  b.max_price,
  b.min_price,

  -- order
  b.cpn_order_id,
  b.order_request_id,
  
  -- first_order with coupon = 0
  case when a.order_request_id_nn = b.order_request_id then 0 else 1 end as f_cnt_coupon
  
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_cpn_users01 as b 
        on a.order_user_id = b.user_id 
          and a.ordered_at_nn < available_start_at_act and available_end_at_act <= ordered_at_nn_31days
;

--- chk
select f_ana01, f_cnt_coupon, count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01 group by rollup(1,2);
select f_ana01, count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01 where order_request_id = order_request_id_nn group by rollup(1);



--[2-1-2] coupon distribution
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist01 as
select
  a.f_ana01,
  a.order_user_id,

  case when b.cnt_coupon is not null then cnt_coupon else 0 end as cnt_coupon,
  case when b.f_coupon is not null then f_coupon else 0 end as f_coupon,
  case when b.cnt_coupon_uq is not null then cnt_coupon_uq else 0 end as cnt_coupon_uq,
  case when b.cnt_campaign is not null then cnt_campaign else 0 end as cnt_campaign,

  -- coupon expiration
  case when b.cnt_exp_term is not null then cnt_exp_term else 0 end as cnt_exp_term,
  case when b.cnt_exp_term_3 is not null then cnt_exp_term_3 else 0 end as cnt_exp_term_3,
  case when b.cnt_exp_term_4 is not null then cnt_exp_term_4 else 0 end as cnt_exp_term_4,
  case when b.cnt_exp_term_7 is not null then cnt_exp_term_7 else 0 end as cnt_exp_term_7,

  -- discount price 
  case when b.cnt_dct_price is not null then cnt_dct_price else 0 end as cnt_dct_price,
  case when b.cnt_discount_price_10 is not null then cnt_discount_price_10 else 0 end as cnt_discount_price_10,
  case when b.cnt_discount_price_20 is not null then cnt_discount_price_20 else 0 end as cnt_discount_price_20,
  case when b.cnt_discount_price_100 is not null then cnt_discount_price_100 else 0 end as cnt_discount_price_100,
  case when b.cnt_discount_price_200 is not null then cnt_discount_price_200 else 0 end as cnt_discount_price_200,
  case when b.cnt_discount_price_300 is not null then cnt_discount_price_300 else 0 end as cnt_discount_price_300,
  case when b.cnt_discount_price_500 is not null then cnt_discount_price_500 else 0 end as cnt_discount_price_500,
  case when b.cnt_discount_price_1000 is not null then cnt_discount_price_1000 else 0 end as cnt_discount_price_1000,
  case when b.cnt_discount_price_2000 is not null then cnt_discount_price_2000 else 0 end as cnt_discount_price_2000,

  -- rate
  case when b.cnt_dct_rate is not null then cnt_dct_rate else 0 end as cnt_dct_rate,
  case when b.cnt_discount_rate_003 is not null then cnt_discount_rate_003 else 0 end as cnt_discount_rate_003,
  case when b.cnt_discount_rate_005 is not null then cnt_discount_rate_005 else 0 end as cnt_discount_rate_005,

  -- min_price
  case when b.cnt_min_price is not null then cnt_min_price else 0 end as cnt_min_price,
  case when b.cnt_min_price_300 is not null then cnt_min_price_300 else 0 end as cnt_min_price_300,
  case when b.cnt_min_price_500 is not null then cnt_min_price_500 else 0 end as cnt_min_price_500,
  case when b.cnt_min_price_2000 is not null then cnt_min_price_2000 else 0 end as cnt_min_price_2000,
  case when b.cnt_min_price_10000 is not null then cnt_min_price_10000 else 0 end as cnt_min_price_10000,

  case when b.cnt_max_price is not null then cnt_max_price else 0 end as cnt_max_price,

  -- passive
  case when b.cnt_passive is not null then cnt_passive else 0 end as cnt_passive,
  case when b.cnt_passive_1 is not null then cnt_passive_1 else 0 end as cnt_passive_1,


from
  (select distinct f_ana01, order_user_id from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01) as a
left join 
  (
    select
      order_user_id,
      
      -- total
      count(coupon_id) as cnt_coupon,
      max(case when coupon_id is not null then 1 else 0 end) as f_coupon,
      count(distinct coupon_id) as cnt_coupon_uq,
      count(distinct name) as cnt_campaign,
      
      -- coupon expiration
      count(expired_term_mst) as cnt_exp_term,
      sum(case when expired_term_mst = 3 then 1 else 0 end) as cnt_exp_term_3,
      sum(case when expired_term_mst = 4 then 1 else 0 end) as cnt_exp_term_4,
      sum(case when expired_term_mst >= 7 then 1 else 0 end) as cnt_exp_term_7,

      -- discount price 
      count(discount_price_mst) as cnt_dct_price,
      sum(case when discount_price_mst = 10 then 1 else 0 end) as cnt_discount_price_10,
      sum(case when discount_price_mst = 20 then 1 else 0 end) as cnt_discount_price_20,
      sum(case when discount_price_mst = 100 then 1 else 0 end) as cnt_discount_price_100,
      sum(case when discount_price_mst = 200 then 1 else 0 end) as cnt_discount_price_200,
      sum(case when discount_price_mst = 300 then 1 else 0 end) as cnt_discount_price_300,
      sum(case when discount_price_mst = 500 then 1 else 0 end) as cnt_discount_price_500,
      sum(case when discount_price_mst = 1000 then 1 else 0 end) as cnt_discount_price_1000,
      sum(case when discount_price_mst = 2000 then 1 else 0 end) as cnt_discount_price_2000,
      
      -- rate
      count(discount_rate_mst) as cnt_dct_rate,
      sum(case when discount_rate_mst = 0.03 then 1 else 0 end) as cnt_discount_rate_003,
      sum(case when discount_rate_mst = 0.05 then 1 else 0 end) as cnt_discount_rate_005,
      
      -- min_price
      count(min_price) as cnt_min_price,
      sum(case when min_price in (300,301) then 1 else 0 end) as cnt_min_price_300,
      sum(case when min_price in (500,501) then 1 else 0 end) as cnt_min_price_500,
      sum(case when min_price in (2000,2001) then 1 else 0 end) as cnt_min_price_2000,
      sum(case when min_price >= 10000 then 1 else 0 end) as cnt_min_price_10000,
      
      count(max_price) as cnt_max_price,

      -- passive
      sum(passive) as cnt_passive,
      sum(case when passive = 1 then 1 else 0 end) as cnt_passive_1,
     
    from
       fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01
    where
      f_cnt_coupon = 1
    group by 1
   ) as b on a.order_user_id = b.order_user_id      
;


--- chk
select discount_rate_mst,discount_price_mst, count(distinct order_user_id) ,count(*) from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist_pt01 group by 1,2 order by 1,2;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist01 where cnt_coupon > 50;

select * from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist01 where 	f_coupon > 0 and cnt_coupon > 1 limit 100;