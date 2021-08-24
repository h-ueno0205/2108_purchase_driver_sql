------- [1-1] balance history
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_balance01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_balance01 as
select
  a.f_ana01,
  a.order_user_id,
  b.*,
  case 
    when a.ymd_nn_6m <= b.actioned_at and b.actioned_at <= a.ymd_nn_31days then '01_6m' 
    when b.actioned_at < a.ymd_nn_6m                                       then '02_before'
    else '99_after'
  end as f_blc_6m
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  (
    select 
      * 
    from 
      fril-analysis-staging-165612.ueno_analysis01.balance_histories01 
    where 
      active = 1
   )@as b on a.order_user_id = b.user_id
;

---- CHK
select active, type_id, sum(amount) from fril-analysis-staging-165612.ueno_analysis02.pd01_balance01 group by 1,2 order by 1,2;
select * from fril-analysis-staging-165612.ueno_analysis01.balance_histories01 where user_id = 8548603;
select * from fril-analysis-staging-165612.ueno_analysis01.balance_histories01 where user_id is null limit 100;

select
  case when b.user_id is null then 1 else 0 end as f,
  active,
  count(distinct a.order_user_id)
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis01.balance_histories01 as b on a.order_user_id = b.user_id and a.ymd_nn_6m <= b.created_at and b.created_at <= a.ymd_nn_31days
group by 1,2
;
select count(distinct order_user_id )  from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01;
select extract(year from created_at)*100+extract(month from created_at), type_id, sum(amount) from fril-analysis-staging-165612.ueno_analysis01.balance_histories01 where type_id = 5 group by 1,2 order by 1,2;


------- [1-2] balance history
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_balance02;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_balance02 as
select
  f_ana01,
  order_user_id,
  user_id,
  type_id,
  f_blc_6m,
  sum(amount) as amount
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_balance01
group by 1,2,3,4,5
order by 1,2,3,4,5
;

select * from fril-analysis-staging-165612.ueno_analysis02.pd01_balance02 where order_user_id = 7832 order by f_blc_6m, type_id;


---- CHK
select * from  fril-analysis-staging-165612.ueno_analysis02.pd01_balance02 limit 10;
select 
  case 
    when user_id is not null then 'have_balance'
    else 'no-blc'
  end as blc,
  type_id,
  count(distinct order_user_id) as uu,
  sum(amount)
from 
  fril-analysis-staging-165612.ueno_analysis02.pd01_balance01
group by rollup(1,2)
order by 1,2
;

select * from fril-analysis-staging-165612.ueno_analysis02.pd01_balance02 where order_user_id = 7832 order by f_blc_6m, type_id;

------- [1-3] balance history
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_balance03;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_balance03 as
select
  f_ana01,
  order_user_id,
  f_blc_6m,
  case when user_id is not null then '01_balace' else '99_not_b' end as f_balance,
  case
    when type_id in (0,1,2,3,6,8) then '01_out_rakuten'
    when type_id in (4) then '02_sales'
    when type_id in (5) then '03_purchase'
    when type_id in (7) then '04_delivery'
    when type_id in (10) then '05_r_cash'
    else '99_uk'
  end as f_blc_type,
  case
    when type_id in (0,1,2,3,6,8) then -1
    when type_id in (4) then 1
    when type_id in (5) then -1
    when type_id in (7) then -1
    when type_id in (10) then -1
    else 1
  end as f_blc_sign,
  sum(amount) as amount
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_balance02
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
;


select * from fril-analysis-staging-165612.ueno_analysis02.pd01_balance03 where order_user_id = 7832 order by f_blc_6m,f_blc_type;