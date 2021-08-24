------- [1] New buyer 2001-2009
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer01 as
select 
  *
from
  fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01_1
where
  f_users = 1
  and
  created_at_ymd between '2020-01-01' and '2020-09-30'
  and
  first_order = 1
;

--- CHK
select count(*), count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer01;
select distinct birth_day from fril-analysis-staging-165612.ueno_analysis01.users01 order by 1;


------- [2]  4 weeks shopping history after becoming Rakuma buyer
-------  memo first_buy‚ª2‰ñ‚Â‚¢‚Ä‚¢‚él‚ª‚¢‚ÄA‚»‚Ì•ª‚Í–µ‚

drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer02;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer02 as
select
  a.order_user_id,
  a.order_request_id as order_request_id_nn,
  b.order_request_id as order_request_id_exist,
  lag(b.order_request_id) over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) as order_request_id_pre,
  a.created_at as ymd_nn,
  b.created_at as ymd_exist,
  a.ordered_at as ordered_at_nn,
  b.ordered_at as ordered_at_exist,
  lag(b.created_at) over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) as created_at_pre,
  a.id as id_nn,
  b.id as id_exist,
  date_diff(b.created_at_ymd, a.created_at_ymd, day) as diff_day,
  div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) as diff_month,
  row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) as seq,
  case 
    when 
      (timestamp_diff(b.created_at, a.created_at, minute) <= 60 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 1)
      then '00_rep_1st_1hour'
    when 
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 1)
      then '01_rep_1st'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 2)
      then '02_rep_2nd'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 3)
      then '03_rep_3rd'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 4)
      then '04_rep_4th'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 5)
      then '05_rep_5th'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) = 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) > 5)
      then '06_rep_6th'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) > 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 1)
      then '07_rep_1st_1Mlater'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) > 0 and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) > 1)
      then '08_rep_2nd_1Mlater'
    when
      (div(date_diff(b.created_at_ymd, a.created_at_ymd, day), 31) is null and row_number() over(partition by a.order_user_id order by a.order_request_id, b.order_request_id) = 1)
      then '09_leave'
    else '99_uk'
  end as f_ana01     
from
  (select * from fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer01) as a
left join   
  (select * from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01_1 where first_order <> 1) as b on a.order_user_id = b.order_user_id
order by 1,2,3,4
;

--- CHK
select
  f_ana01,
  count(distinct order_user_id) as uu,
  count(*) as n_row
from
 fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer02
group by rollup(1)
order by 1
;
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer02 where order_user_id in (13692664,8237866,14677741) order by 1,2;
select * from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01 where order_user_id = 13692664 order by 1;




select * from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01 where order_user_id = 13692664 order by 1;
select * from fril-analysis-staging-165612.ueno_analysis01.items01 where id in (290359579,294875812);
select count(distinct order_user_id), sum(first_order) from fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01;




------- [3] Analysis users
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_ana01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as
select
  a.order_user_id,
  a.order_request_id_nn,
  a.order_request_id_exist,
  a.ymd_nn,
  a.ymd_exist,
  a.ordered_at_nn,
  a.ordered_at_exist,
  case when a.ymd_exist is null then date_add(a.ymd_nn, interval 31 day) else a.ymd_exist end as ymd_nn_31days,
  date_sub(a.ymd_nn, interval 180 day) as ymd_nn_6m,
  date_add(a.ordered_at_nn, interval 31 day) as ordered_at_nn_31days,
  date_sub(a.ordered_at_nn, interval 180 day) as ordered_at_nn_6m,
  a.id_nn,
  a.id_exist,
  a.diff_day,
  a.diff_month,
  a.seq,
  a.f_ana01,
  b.gender,
  2021 - extract(year from b.birth_day) as age,
  c.genre_id as genre_id_nn,
  c.parent_category_id as parent_category_id_nn,
  c.category_id as category_id_nn, 
  c.sell_price as sell_price_nn,
  c.coupon_discount_price,
  c.use_point_num,
  c.use_balance_num,
  d.clst as clst_nn
 
from
 (select * from fril-analysis-staging-165612.ueno_analysis02.pd01_newbuyer02 where f_ana01 in ('01_rep_1st','09_leave')) as a
inner join
  fril-analysis-staging-165612.ueno_analysis01.users01 as b on a.order_user_id = b.id
inner join  
  fril-analysis-staging-165612.ueno_analysis01.accounting_soldout_items01_1 as c on a.order_request_id_nn = c.order_request_id
left join
  fril-analysis-staging-165612.ueno_analysis01.cust_seg_category_cluster01 as d on c.category_id = d.cate_id
; 



--- CHK
select
  clst_nn,
  f_ana01,
  count(distinct order_user_id) as uu,
  count(*)
from 
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01
group by rollup(1,2)
order by 1,2
;
  
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 where order_request_id_exist is not null limit 100;
  
  
------- [4] item
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_sell01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_sell01 as
select
  a.f_ana01,
  a.order_user_id,
  b.*,
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis01.items01 as b on a.order_user_id = b.user_id and a.ymd_nn_6m <= b.created_at and b.created_at <= a.ymd_nn_31days
;

--- CHK
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_sell01 limit 10;

select
  a.f_ana01,
  a.sold_out_flag,
  count(distinct b.user_id) as uu,
  sum(a.frq) as frq,
  sum(a.gms) as gms
from
  (
    select
      f_ana01,
      user_id,
      sold_out_flag,
      count(*) as frq,
      sum(sell_price) as gms
    from 
      fril-analysis-staging-165612.ueno_analysis02.pd01_sell01
    where
      user_id is not null
    group by 1,2,3
  ) as a
left join
  (
    select
      user_id,
      max(sold_out_flag) as so
    from 
      fril-analysis-staging-165612.ueno_analysis02.pd01_sell01
    where
      user_id is not null
    group by 1
) as b
on a.user_id = b.user_id and a.sold_out_flag = b.so
group by rollup(1,2)
order by 1,2
;

select 
  extract(year from created_at)*100 + extract(month from created_at) as yyyymm,
  sold_out_flag,
  count(distinct user_id), count(*), sum(sell_price)
from 
  fril-analysis-staging-165612.ueno_analysis01.items01 as aa1
where
  exists(select id from fril-analysis-staging-165612.ueno_analysis01.users01 as aa2 where f_users = 1 and aa2.id = aa1.user_id)
  and
  created_at_ymd between '2019-10-01' and '2020-09-30'
group by rollup(1,2)
order by 1,2;