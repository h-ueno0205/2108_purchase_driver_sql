------- [1-1] search query
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_sc_qry01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_sc_qry01 as
  select
    id,
    user_id,
    query, 
    active,
    notification_frequency,
    merged_search_query_id,
    created_at,
    updated_at,
    json_extract_scalar(query, '$.text') as txt,
    json_query(query, '$.statuses') as status,
    json_query(query, '$.brand_ids') as brand,
    json_query(query, '$.parent_category_ids') as parent_category,
    json_query(query, '$.category_ids') as category,
    json_query(query, '$.color_ids') as color,
    json_query(query, '$.size_ids') as size,
    json_query(query, '$.size_group_id') as size_group,
    json_query(query, '$.min_sell_price') as min_sell_price,
    json_query(query, '$.max_sell_price') as max_sell_price,
    json_extract_scalar(query, '$.transaction_status') as tran_status
  from 
    fril-analysis-staging-165612.ueno_analysis01.search_queries01 
  where 
    date(created_at) between '2019-12-01' and '2020-10-30'
;



------- [2-1] payment by Rakuten Super Point / rakuten cash
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd01_rsp_rc01;
create table fril-analysis-staging-165612.ueno_analysis02.pd01_rsp_rc01 as
  select
    id,
    order_request_id,
    user_id,
    item_id
    settlement_id,
    point,
    priority,
    point_usage,
    status,
    result,
    settlement_commission,
    created_at,
    updated_at,
  from 
    fril-analysis-staging-165612.ueno_analysis01.rakuten_point_settlements01
  where 
    date(created_at) between '2019-12-01' and '2020-10-30'
    and
    status = 5
;