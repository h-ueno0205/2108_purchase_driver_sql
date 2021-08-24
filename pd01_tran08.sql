------- [1-1] inflow channel
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_inflow01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_inflow01 as
select
  a.f_ana01,
  a.order_user_id,
  min(case
    when b.registration_type = 1 then '01_web'
    when b.registration_type = 0 then '00_app'
    else '03_uk'
  end) as inflow_channel,
  min(b.registration_type) as f_inflow,
  min(b.created_at) as created_at
from
  fril-analysis-staging-165612.ueno_analysis02.pd01_ana01 as a
inner join
  fril-analysis-staging-165612.ueno_analysis01.user_statuses01 as b on a.order_user_id = b.user_id
group by 1,2
order by 1,2
;

select
  f_ana01,
  f_inflow,
  count(*)
from
 fril-analysis-staging-165612.ueno_analysis02.pd01_inflow01
group by rollup(1,2)
order by 1,2
;