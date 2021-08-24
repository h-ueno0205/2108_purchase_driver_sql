-------------------------------------------------------- [1] Target
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_tgt01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_tgt01 as
select
  order_user_id,
  f_ana01,
  date_diff(ymd_nn_31days, ymd_nn, day)+1 as date_diff_nn,
  case
    when f_ana01 = '01_rep_1st' then 1
    when f_ana01 = '09_leave' then 0
    else 99
  end as f_tgt
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01
;

--- chk
select f_ana01, count(*), count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd02_tgt01 group by 1;
select distinct age from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 order by 1;
select f_ana01, avg(date_diff_nn), count(*) from fril-analysis-staging-165612.ueno_analysis02.pd02_tgt01 group by 1;


-------------------------------------------------------- [1] buy_demo
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_buy_demo01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_buy_demo01 as
select
  order_user_id,
  
  --- gender
  case when gender = 1 then 1 else 0 end as f_male,
  case when gender = 2 then 1 else 0 end as f_female,
  case when gender = 0 then 1 else 0 end as f_gender_null,

  --- first_buy_price
  case when sell_price_nn <= 1000                    then 1 else 0 end as f_fb_pr_1k,
  case when sell_price_nn between 1001   and  3000   then 1 else 0 end as f_fb_pr_3k,
  case when sell_price_nn between 3001   and  5000   then 1 else 0 end as f_fb_pr_5k,
  case when sell_price_nn between 5001   and  10000  then 1 else 0 end as f_fb_pr_10k,
  case when sell_price_nn between 10001  and  25000  then 1 else 0 end as f_fb_pr_25k,
  case when sell_price_nn between 25001  and  50000  then 1 else 0 end as f_fb_pr_50k,
  case when sell_price_nn between 50001  and  100000 then 1 else 0 end as f_fb_pr_100k,
  case when sell_price_nn between 100001 and  200000 then 1 else 0 end as f_fb_pr_200k,
  case when sell_price_nn >= 200001 then 1 else 0 end as f_fb_pr_201k,
  
  --- cluster
  case when clst_nn = 'A' then 1 else 0 end as f_clst_fb_a,
  case when clst_nn = 'B' then 1 else 0 end as f_clst_fb_b,
  case when clst_nn = 'C' then 1 else 0 end as f_clst_fb_c,
  case when clst_nn = 'D' then 1 else 0 end as f_clst_fb_d,
  case when clst_nn = 'E' then 1 else 0 end as f_clst_fb_e,
  case when clst_nn = 'F' then 1 else 0 end as f_clst_fb_f,
  case when clst_nn = 'G' then 1 else 0 end as f_clst_fb_g,
  case when clst_nn = 'H' then 1 else 0 end as f_clst_fb_h,
  case when clst_nn = 'I' then 1 else 0 end as f_clst_fb_i,
  case when clst_nn = 'J' then 1 else 0 end as f_clst_fb_j,
  case when clst_nn = 'K' then 1 else 0 end as f_clst_fb_k,
  case when clst_nn = 'L' then 1 else 0 end as f_clst_fb_l,
  
  --- age
  case when (age is null) or (age < 20) or (age >= 70) then 1 else 0 end as f_age_null,
  case when age between 20 and 29 then 1 else 0 end as f_age_20,
  case when age between 30 and 39 then 1 else 0 end as f_age_30,
  case when age between 40 and 49 then 1 else 0 end as f_age_40,
  case when age between 50 and 59 then 1 else 0 end as f_age_50,
  case when age between 60 and 69 then 1 else 0 end as f_age_60,

  --- category
  case when genre_id_nn = 10001 then 1 else 0 end as f_fb_lady,
  case when genre_id_nn = 10002 then 1 else 0 end as f_fb_other,
  case when genre_id_nn = 10003 then 1 else 0 end as f_fb_kid_mty,
  case when genre_id_nn = 10004 then 1 else 0 end as f_fb_cosme,
  case when genre_id_nn = 10005 then 1 else 0 end as f_fb_mens,
  case when genre_id_nn = 10006 then 1 else 0 end as f_fb_smp_app,
  case when genre_id_nn = 10007 then 1 else 0 end as f_fb_ent_hby,
  case when genre_id_nn = 10008 then 1 else 0 end as f_fb_ticket,
  case when genre_id_nn = 10009 then 1 else 0 end as f_fb_interior,
  case when genre_id_nn = 10010 then 1 else 0 end as f_fb_handmade,
  case when genre_id_nn = 10011 then 1 else 0 end as f_fb_car,
  case when genre_id_nn = 10012 then 1 else 0 end as f_fb_food_drink,
  case when genre_id_nn = 10013 then 1 else 0 end as f_fb_instr,
  case when genre_id_nn = 10014 then 1 else 0 end as f_fb_sports

from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01
;




select distinct genre_id, genre_name from fril-analysis-staging-165612.ueno_analysis01.cust_seg_category_cluster01 order by 1;
select f_fb_pr_1k, f_fb_pr_3k, count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd02_buy_demo01 group by 1,2 order by 1,2;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_buy_demo01 where f_fb_pr_1k = 1 and f_fb_pr_3k=1 limit 10;
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 where order_user_id in (7481182,14742723);


-------------------------------------------------------- [2] sell 
--------- part01
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt01 as
select
  a.f_ana01,
  a.order_user_id,
  
  --- sell
  a.sell_frq,
  a.sell_gms,
  case when a.sell_frq > 0 then a.sell_gms / a.sell_frq else 0 end as sell_avg_price,
  case when a.sell_frq > 0 then a.sell_discount / a.sell_frq else 0 end as sell_avg_discount,

--- soldout
  a.so_frq,
  a.so_gms,
  case when a.so_frq > 0 then a.so_gms / a.so_frq else 0 end as so_avg_price,
  case when a.so_frq > 0 then a.so_discount / a.so_frq else 0 end as so_avg_discount,

--- soldout_ratio
  case when a.sell_frq > 0 then a.so_frq / a.sell_frq else 0 end as so_frq_ratio,
  case when a.sell_gms > 0 then a.so_gms / a.sell_gms else 0 end as so_gms_ratio

from
  (
    select 
      f_ana01,
      order_user_id,
      sum(case when user_id is not null then 1 else 0 end) as sell_frq,
      sum(case when user_id is not null then sell_price else 0 end) as sell_gms,
      sum(case when user_id is not null then origin_price - sell_price else 0 end) as sell_discount,
      sum(case when sold_out_flag = 1 then 1 else 0 end) as so_frq,
      sum(case when sold_out_flag = 1 then sell_price else 0 end) as so_gms,
      sum(case when sold_out_flag = 1 then origin_price - sell_price else 0 end) as so_discount
    from 
      fril-analysis-staging-165612.ueno_analysis02.pd01_sell01
    group by 1,2
  ) as a
order by 1,2
;

--------- part02
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt02;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt02 as
select
  a.order_user_id,
  
  --- category flagged sell
  max(case when genre_id = 10001 then 1 else 0 end) as f_sl_lady,
  max(case when genre_id = 10002 then 1 else 0 end) as f_sl_other,
  max(case when genre_id = 10003 then 1 else 0 end) as f_sl_kid_mty,
  max(case when genre_id = 10004 then 1 else 0 end) as f_sl_cosme,
  max(case when genre_id = 10005 then 1 else 0 end) as f_sl_mens,
  max(case when genre_id = 10006 then 1 else 0 end) as f_sl_smp_app,
  max(case when genre_id = 10007 then 1 else 0 end) as f_sl_ent_hby,
  max(case when genre_id = 10008 then 1 else 0 end) as f_sl_ticket,
  max(case when genre_id = 10009 then 1 else 0 end) as f_sl_interior,
  max(case when genre_id = 10010 then 1 else 0 end) as f_sl_handmade,
  max(case when genre_id = 10011 then 1 else 0 end) as f_sl_car,
  max(case when genre_id = 10012 then 1 else 0 end) as f_sl_food_drink,
  max(case when genre_id = 10013 then 1 else 0 end) as f_sl_instr,
  max(case when genre_id = 10014 then 1 else 0 end) as f_sl_sports

from
  (
    select 
      f_ana01,
      order_user_id,
      a2.genre_id,
      sum(case when user_id is not null then 1 else 0 end) as sell_frq,
      sum(case when user_id is not null then sell_price else 0 end) as sell_gms,
      sum(case when user_id is not null then origin_price - sell_price else 0 end) as sell_discount,
      sum(case when sold_out_flag = 1 then 1 else 0 end) as so_frq,
      sum(case when sold_out_flag = 1 then sell_price else 0 end) as so_gms,
      sum(case when sold_out_flag = 1 then origin_price - sell_price else 0 end) as so_discount
    from 
      fril-analysis-staging-165612.ueno_analysis02.pd01_sell01 as a1
    left join
      fril-analysis-staging-165612.ueno_analysis01.cust_seg_category_cluster01 as a2 on a1.category_id = a2.cate_id
    group by 1,2,3
  ) as a
group by 1
order by 1
;

-- chk
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt02 limit 100;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt02 where order_user_id in (13762125,15356388,15438690);


--------- sell summary
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_sell01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_sell01 as
select
  a.*,
  b.f_sl_lady,
  b.f_sl_other,
  b.f_sl_kid_mty,
  b.f_sl_cosme,
  b.f_sl_mens,
  b.f_sl_smp_app,
  b.f_sl_ent_hby,
  b.f_sl_ticket,
  b.f_sl_interior,
  b.f_sl_handmade,
  b.f_sl_car,
  b.f_sl_food_drink,
  b.f_sl_instr,
  b.f_sl_sports	

from
  fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt01 as a
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_sell_pt02 as b on a.order_user_id = b.order_user_id
order by 1,2