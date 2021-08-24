------- [1] karte POP UP
-- Memo
-- karte_eventテーブルへのクエリを作成する
-- https://developers.karte.io/docs/datahub-karte_event-table#%E6%8A%BD%E5%87%BA%E6%9C%9F%E9%96%93%E3%82%92%E6%8C%87%E5%AE%9A%E3%81%97%E3%81%9F%E3%82%AF%E3%82%A8%E3%83%AA%E5%AE%9F%E8%A1%8C%E6%96%B9%E6%B3%95


------- [1-1] 
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_karte01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_karte01 as
select
  a.f_ana01,
  a.order_user_id,
  count(distinct campaign_id) as cnt_karte_uq,
  count(campaign_id) as cnt_karte,
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01　as a 
left join
  fril-analysis-staging-165612.ueno_analysis02.pd01_karte01　as b on a.order_user_id = b.user_id and a.ymd_nn < b.sync_date and b.sync_date <= ymd_nn_31days
group by 1,2
order by 1,2
;