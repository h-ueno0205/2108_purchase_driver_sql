------- [1-1] Coupon list
drop table if exists fril-analysis-staging-165612.ueno_analysis02.coupon_list02;
create table fril-analysis-staging-165612.ueno_analysis02.coupon_list02 as
select
  campaign_desc_uq,
  coupon_id,
  max(boost_flag) as boost_flag, 
  max(buy_existing) as buy_existing,
  max(buy_new) as buy_new,
  max(buy_return) as buy_return,
  max(buy_other) as buy_other,
  max(sell_new) as sell_new,
  max(sell_return) as sell_return,
  max(sell_other) as sell_other,
  max(other_Mass) as other_Mass,
  max(other_other) as other_other
from
  fril-analysis-staging-165612.ueno_analysis02.coupon_list01
group by 1,2
;

------- [1-2] Distributed and Used coupon w/o cancel
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_cpn_users01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_cpn_users01 as
select
    a.cpn_dst_id,
    a.user_id,
    a.coupon_id,
    a.name,
    a.available_start_at_act,
    a.available_end_at_act,
    a.expired_term_mst,
    a.discount_price_mst,
    a.discount_rate_mst,
    a.passive,
    a.max_price,
    a.min_price,
    a.created_at_mst,
    a.updated_at_mst,
    a.created_at_deliver,
    a.updated_at_deliver,
    
    -- Order
    b.id as cpn_order_id,
    b.order_request_id,
    b.discount_price as discount_price_act,	
    b.created_at as created_at_order,
    b.updated_at as updated_at_order,

    -- coupon_desc
    c.campaign_desc_uq,
    c.boost_flag,
    c.buy_existing,
    c.buy_new,
    c.buy_return,
    c.buy_other,
    c.sell_new,
    c.sell_return,
    c.sell_other,
    c.other_Mass,
    c.other_other	
    
from
  (
      select 
          a1.id as cpn_dst_id,
          a1.user_id,
          a1.coupon_id,
          a2.name,
          a1.available_start_at as available_start_at_act,
          a1.available_end_at as available_end_at_act,
          a2.expired_term as expired_term_mst,
          a2.discount_price as discount_price_mst,
          a2.discount_rate as discount_rate_mst,
          a2.passive,
          a2.max_price,
          a2.min_price,
          a2.created_at as created_at_mst,
          a2.updated_at as updated_at_mst,
          a1.created_at as created_at_deliver,
          a1.updated_at as updated_at_deliver
      from 
          fril-analysis-staging-165612.ueno_analysis01.user_coupons01 as a1
      inner join
          fril-analysis-staging-165612.ueno_analysis01.coupons01 as a2 on a1.coupon_id = a2.id
      where 
        date(a1.available_start_at) >= '2019-12-01' and date(a1.available_end_at) <= '2020-10-30'
        and
        exists(select order_user_id from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a3 where a1.user_id = a3.order_user_id)
   ) as a
left join
  (
      select
        *
      from
         fril-analysis-staging-165612.ueno_analysis01.order_user_coupons01 as b1
      where
        exists(select order_request_id from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01_1 as b2 where b1.order_request_id = b2.order_request_id)
   ) as b on a.cpn_dst_id = b.user_coupon_id
left join
       fril-analysis-staging-165612.ueno_analysis02.coupon_list02 as c on a.coupon_id = c.coupon_id
;

select
  *
from
  fril-analysis-staging-165612.ueno_analysis01.order_user_coupons01
where
  order_request_id in (89224765,89118522)
;
