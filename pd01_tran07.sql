------- [1] Accesslog
---- [1-1] all customers
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_allcust01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_allcust01 as
select
  distinct order_user_id
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01
order by 1
;



---- [1-2] Item / ssn and view raw01
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_item_view01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_item_view01 as
select
  a.user_id,
  cast(format_timestamp('%Y-%m-%d %H:%M:%S', ifnull(a.pre_iv_utc,a.cur_iv_utc), 'Asia/Tokyo') as datetime) as pre_iv,
  cast(format_timestamp('%Y-%m-%d %H:%M:%S', a.cur_iv_utc, 'Asia/Tokyo') as datetime) as cur_iv,
  ifnull(a.pre_iv_utc, a.cur_iv_utc) as pre_iv_utc,
  a.cur_iv_utc,
  a.f_diff_min,
  a.item_id,
  a.web,
  sum(a.f_diff_min) over(partition by a.user_id order by a.cur_iv_utc) + 1 as ssn_no,
  case
    when a.f_diff_min = 0 then timestamp_diff(a.cur_iv_utc, ifnull(a.pre_iv_utc, a.cur_iv_utc), second) 
    else 0 
  end as iv_sec,
  case
    when a.f_diff_min = 1 then timestamp_diff(a.cur_iv_utc, ifnull(a.pre_iv_utc, a.cur_iv_utc), minute) 
    else 0
  end as leave_min,
  case
    when a.f_diff_min = 1 then timestamp_diff(a.cur_iv_utc, ifnull(a.pre_iv_utc, a.cur_iv_utc), hour) 
    else 0
  end as leave_hour
--  row_number() over(partition by a.user_id  order by a.cur_iv_utc) as item_seq_user
 from
  (
    select
      lag(timestamp) over (partition by user_id order by timestamp) as pre_iv_utc,
      timestamp as cur_iv_utc,
    case  
        when timestamp_diff(timestamp, ifnull(lag(timestamp) over (partition by user_id order by timestamp),timestamp), minute) >= 30 then 1
        else 0
     end as f_diff_min,
       *  
    from
      `stellar-fx-826.analysis.item_view_*` as a1
    where
      _TABLE_SUFFIX between FORMAT_DATE("%Y%m%d", DATE('2019-12-01')) and FORMAT_DATE("%Y%m%d", DATE('2020-10-30'))
      and
       user_id is not null
      and 
      exists(select order_user_id from fril-analysis-staging-165612.ueno_analysis02.pd01_allcust01 as a2 where a1.user_id = a2.order_user_id)
  ) as a
;


---- [1-2] Item session / view raw02
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_item_view02;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_item_view02 as
select
  a1.user_id,
  a1.ssn_no,
  a1.cur_iv,
  a1.cur_iv_utc,
  a1.item_id,
  a1.web,
  a1.iv_sec,
  a1.leave_min,
  a1.leave_hour,
  row_number() over(partition by a1.user_id, a1.ssn_no order by a1.cur_iv_utc) as item_seq_ssn
from
    fril-analysis-staging-165612.ueno_analysis02.pd01_item_view01 as a1
order by 1,2,3,4
;