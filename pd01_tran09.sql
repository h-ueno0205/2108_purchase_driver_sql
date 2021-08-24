--- Memo
--- Query for karte event 
--- https://developers.karte.io/docs/datahub-karte_event-table#%E6%8A%BD%E5%87%BA%E6%9C%9F%E9%96%93%E3%82%92%E6%8C%87%E5%AE%9A%E3%81%97%E3%81%9F%E3%82%AF%E3%82%A8%E3%83%AA%E5%AE%9F%E8%A1%8C%E6%96%B9%E6%B3%95


------- [1-1] karte POP UP
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_karte01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_karte01 as
select
  cast(user_id as int64) as user_id,  
  session_id,
  sync_date,
  event_name,
  json_query(`values`, '$.message_open.message.campaign_id') as campaign_id,
  json_query(`values`, '$.message_open.message.shorten_id') as shorten_id,
  `values`
from
  `karte-data.karte_stream_89ab6ad3d05637cd386be6e5dc175527.krt_pockyevent_v1_*` as a
where
  _TABLE_SUFFIX between FORMAT_DATE("%Y%m%d", DATE('2019-12-01')) and FORMAT_DATE("%Y%m%d", DATE('2020-10-30'))
  and
  event_name = 'message_open' 
  and
  regexp_contains(user_id, '^[0-9]+')
  and
  exists (select order_user_id from fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as b where cast(user_id as int64) = b.order_user_id)
;