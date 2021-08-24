------- [1-1] notification summaries
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_msg01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_msg01 as
select
  *,
  case
    when type_id = 1 then '01_like'
    when type_id = 2 then '02_comment'
    when type_id = 3 then '03_follower'
    when type_id = 21 then '04_discount'
    when type_id = 25 then '05_message'
    when type_id = 26 then '05_message'
    else '99_uk'
  end as f_type,
  case
      -- type_id = 25
      when type_id = 25 and (message like '%オフクーポン%' or message like '%OFFクーポン%')                                          then '01_coupon'
      when type_id = 25 and (message like '%売上金%' or message like '%楽天キャッシュ%')                                             then '02_sales_r_cash'
      when type_id = 25 and (message like '%招待ポイント%')                                                                          then '03_invitation'
      when type_id = 25 and (message like '%ラクマへのご登録%')                                                                      then '04_welcome' 
      when type_id = 25 and (message like '%初めてのご出品%')                                                                        then '05_new_seller'
      when type_id = 25 and (message like '%アンケート%' or message like '%インタビュー%')                                           then '06_survey'
      when type_id = 25 and (message like '%受取%')                                                                                  then '07_review'
      when type_id = 25 and (message like '%楽天%'or message like '%Paidy決済%'or message like '%ラクマ公式中古スマホショップ%')     then '08_ad'
      when type_id = 25 and (message like '%JKが選ぶ%' or message like '%リモートオタ%' or message like '%ラクマラボ%')              then '09_pr'
      when 
           type_id = 25 and (message like '%ラクマ%' or message like '%重要%'   or message like '%お知らせ%' 
                              or message like '%ご案内%' or message like '%ご報告%' or message like '%本人確認%')                    then '10_rakuma_msg'
      when type_id = 25 and message like '%キャンセル%'                                                                              then '11_cancel'
      
      -- type_id = 26
      when type_id = 26 and message like '%有効期限%'                                                                                then '12_coupon_expiration'    
      when type_id = 26 and message like '%返金%'                                                                                    then '13_refund'    
      when type_id = 26 and (message like '%アプリDL特典%')                                                                          then '03_invitation'
      when type_id in (25,26)                                                                                                        then '19_msg_other'
      else '99_uk'
  end as f_msg
from 
  fril-analysis-staging-165612.ueno_analysis01.notification_summaries01 
where 
  date(created_at) between '2020-01-01' and '2020-10-31'
order by 1
;

---- CHK
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_msg01 limit 100;
select
  f_type,
  f_msg,
  count(*) as cnt_msg,
  count(distinct user_id) as uu
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_msg01
group by rollup(1,2)
order by 1,2
;


------- [1-2] notification summaries2
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_msg02;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_msg02 as
select 
  a.f_ana01,
  a.order_user_id,
  b.source_user_id,  
  b.item_id,	
  b.type_id,
  b.count,
  b.message,
  b.read,
  b.active,
  b.created_at,
  b.f_type,
  b.f_msg,
  a.ymd_nn,
  a.ymd_nn_31days
from 
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd01_msg01 as b 
            on a.order_user_id = b.user_id and a.ymd_nn <= b.created_at and b.created_at <= a.ymd_nn_31days
;


select * from fril-analysis-staging-165612.ueno_analysis02.pd01_msg02 where f_ana01  <> '09_leave' order by 2  limit 100 ; 


select * from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 where f_ana01  <> '09_leave' limit 100;
