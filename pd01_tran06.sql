------- [1-1] Rakuten user / rank
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_rtn01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_rtn01 as
select
  a.order_user_id,
  b.easy_id,
  b.last_sign_in_at,
  b.created_at,
  b.updated_at,
  a.ordered_at_nn,
  a.ymd_nn,
  a.ymd_nn_31days,
  case when created_at <= ymd_nn_31days then 1 else 0 end as f_rtn
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
left join
  fril-analysis-staging-165612.ueno_analysis01.rakuten_users01 as b on a.order_user_id = b.user_id
;

