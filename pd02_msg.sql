----- [1] message
--- [1-1] total cnt by order_user_id
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_msg_pt01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_msg_pt01 as
select
  f_ana01,
  order_user_id,
  date_diff(ymd_nn_31days, ymd_nn, day) + 1 as diff_days,
  count(distinct item_id) as cnt_item,
  sum(case when f_type = '01_like'            then 1 else 0 end) as cnt_msg_like,
  sum(case when f_type = '02_comment'         then 1 else 0 end) as cnt_msg_comment,
  sum(case when f_type = '03_follower'        then 1 else 0 end) as cnt_msg_follower,
  sum(case when f_type = '04_discount'        then 1 else 0 end) as cnt_msg_discount,
  sum(case when f_msg  = '01_coupon'          then 1 else 0 end) as cnt_msg_coupon,
  sum(case when f_msg  = '02_sales_r_cash'    then 1 else 0 end) as cnt_msg_sales_to_r_cash,
  sum(case when f_msg  = '03_invitation'      then 1 else 0 end) as cnt_msg_invitation,
  sum(case when f_msg  = '04_welcome'         then 1 else 0 end) as cnt_msg_welcome,
  sum(case when f_msg  = '05_new_seller'      then 1 else 0 end) as cnt_msg_new_seller,
  sum(case when f_msg  = '06_survey'          then 1 else 0 end) as cnt_msg_survey,
  sum(case when f_msg  = '08_ad'              then 1 else 0 end) as cnt_msg_ad,
  sum(case when f_msg  = '09_pr'              then 1 else 0 end) as cnt_msg_pr,
  sum(case when f_msg  = '10_rakuma_msg'      then 1 else 0 end) as cnt_msg_rakuma_msg,
  sum(case when f_msg  = '11_cancel'          then 1 else 0 end) as cnt_msg_cancel,
  sum(case when f_msg  = '12_coupon_expiration'    then 1 else 0 end) as cnt_msg_coupon_expiration,
  sum(case when f_msg  = '13_refund'               then 1 else 0 end) as cnt_msg_refund,
  sum(case when f_msg  = '19_msg_other'            then 1 else 0 end) as cnt_msg_other
from 
  fril-analysis-staging-165612.ueno_analysis02.pd01_msg02 
-- where
--   order_user_id = 14777618
group by 1,2,3
order by 1,2,3
;


--- chk
select diff_days, count(*) from fril-analysis-staging-165612.ueno_analysis02.pd02_msg_pt01 group by 1 order by 1;
select count(distinct order_user_id) from fril-analysis-staging-165612.ueno_analysis02.pd01_msg02;


--- [1-2] total cnt by order_user_id
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_msg01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_msg01 as
select
  a.f_ana01,
  a.order_user_id,
  
  --- actual
  case when b.diff_days is not null                       then 1                            else 0 end as f_msg,
  case when b.diff_days is not null                       then b.diff_days                  else 0 end as cnt_msg_diff_days,
  case when b.cnt_item is not null                        then b.cnt_item                   else 0 end as cnt_msg_item,
  case when b.cnt_msg_like is not null                    then b.cnt_msg_like               else 0 end as cnt_msg_like,
  case when b.cnt_msg_comment is not null                 then b.cnt_msg_comment            else 0 end as cnt_msg_comment,
  case when b.cnt_msg_follower is not null                then b.cnt_msg_follower           else 0 end as cnt_msg_follower,
  case when b.cnt_msg_discount is not null                then b.cnt_msg_discount           else 0 end as cnt_msg_discount,
  case when b.cnt_msg_coupon is not null                  then b.cnt_msg_coupon             else 0 end as cnt_msg_coupon,
  case when b.cnt_msg_sales_to_r_cash is not null         then b.cnt_msg_sales_to_r_cash    else 0 end as cnt_msg_to_r_cash,
  case when b.cnt_msg_invitation is not null              then b.cnt_msg_invitation         else 0 end as cnt_msg_invitation,
  case when b.cnt_msg_welcome is not null                 then b.cnt_msg_welcome            else 0 end as cnt_msg_welcome,
  case when b.cnt_msg_new_seller is not null              then b.cnt_msg_new_seller         else 0 end as cnt_msg_new_seller,
  case when b.cnt_msg_survey is not null                  then b.cnt_msg_survey             else 0 end as cnt_msg_survey,
  case when b.cnt_msg_ad is not null                      then b.cnt_msg_ad                 else 0 end as cnt_msg_ad,
  case when b.cnt_msg_pr is not null                      then b.cnt_msg_pr                 else 0 end as cnt_msg_pr,
  case when b.cnt_msg_rakuma_msg is not null              then b.cnt_msg_rakuma_msg         else 0 end as cnt_msg_rakuma_msg,
  case when b.cnt_msg_cancel is not null                  then b.cnt_msg_cancel             else 0 end as cnt_msg_cancel,
  case when b.cnt_msg_coupon_expiration is not null       then b.cnt_msg_coupon_expiration  else 0 end as cnt_msg_coupon_expiration,
  case when b.cnt_msg_refund is not null                  then b.cnt_msg_refund             else 0 end as cnt_msg_refund,
  case when b.cnt_msg_other is not null                   then b.cnt_msg_other              else 0 end as cnt_msg_other,

  --- standardized by #days
  case when b.diff_days is not null then b.cnt_item/b.diff_days                   else 0 end as cnt_msg_item_std,
  case when b.diff_days is not null then b.cnt_msg_like/b.diff_days               else 0 end as cnt_msg_like_std,
  case when b.diff_days is not null then b.cnt_msg_comment/b.diff_days            else 0 end as cnt_msg_comment_std,
  case when b.diff_days is not null then b.cnt_msg_follower/b.diff_days           else 0 end as cnt_msg_follower_std,
  case when b.diff_days is not null then b.cnt_msg_discount/b.diff_days           else 0 end as cnt_msg_discount_std,
  case when b.diff_days is not null then b.cnt_msg_coupon/b.diff_days             else 0 end as cnt_msg_coupon_std,
  case when b.diff_days is not null then b.cnt_msg_sales_to_r_cash/b.diff_days    else 0 end as cnt_msg_to_r_cash_std,
  case when b.diff_days is not null then b.cnt_msg_invitation/b.diff_days         else 0 end as cnt_msg_invitation_std,
  case when b.diff_days is not null then b.cnt_msg_welcome/b.diff_days            else 0 end as cnt_msg_welcome_std,
  case when b.diff_days is not null then b.cnt_msg_new_seller/b.diff_days         else 0 end as cnt_msg_new_seller_std,
  case when b.diff_days is not null then b.cnt_msg_survey/b.diff_days             else 0 end as cnt_msg_survey_std,
  case when b.diff_days is not null then b.cnt_msg_ad/b.diff_days                 else 0 end as cnt_msg_ad_std,
  case when b.diff_days is not null then b.cnt_msg_pr/b.diff_days                 else 0 end as cnt_msg_pr_std,
  case when b.diff_days is not null then b.cnt_msg_rakuma_msg/b.diff_days         else 0 end as cnt_msg_rakuma_msg_std,
  case when b.diff_days is not null then b.cnt_msg_cancel/b.diff_days             else 0 end as cnt_msg_cancel_std,
  case when b.diff_days is not null then b.cnt_msg_coupon_expiration/b.diff_days  else 0 end as cnt_msg_coupon_expiration_std,
  case when b.diff_days is not null then b.cnt_msg_refund/b.diff_days             else 0 end as cnt_msg_refund_std,
  case when b.diff_days is not null then b.cnt_msg_other/b.diff_days              else 0 end as cnt_msg_other_std

from 
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join fril-analysis-staging-165612.ueno_analysis02.pd02_msg_pt01 as b on a.order_user_id = b.order_user_id
;



--- chk
select * from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 where order_user_id in (282915,9780490);