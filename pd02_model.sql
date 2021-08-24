------- [1] For model
drop table if exists fril-analysis-staging-165612.ueno_analysis02.pd02_model01;
create table fril-analysis-staging-165612.ueno_analysis02.pd02_model01 as
select
  -- pd02_tgt01
  a.order_user_id,
  a.f_ana01,
  a.date_diff_nn,
  a.f_tgt,

  --pd02_buy_demo01
  b.f_male,
  b.f_female,
  b.f_gender_null,
  b.f_fb_pr_1k,
  b.f_fb_pr_3k,
  b.f_fb_pr_5k,
  b.f_fb_pr_10k,
  b.f_fb_pr_25k,
  b.f_fb_pr_50k,
  b.f_fb_pr_100k,
  b.f_fb_pr_200k,
  b.f_fb_pr_201k,
  b.f_clst_fb_a,
  b.f_clst_fb_b,
  b.f_clst_fb_c,
  b.f_clst_fb_d,
  b.f_clst_fb_e,
  b.f_clst_fb_f,
  b.f_clst_fb_g,
  b.f_clst_fb_h,
  b.f_clst_fb_i,
  b.f_clst_fb_j,
  b.f_clst_fb_k,
  b.f_clst_fb_l,
  b.f_age_null,
  b.f_age_20,
  b.f_age_30,
  b.f_age_40,
  b.f_age_50,
  b.f_age_60,
  b.f_fb_lady,
  b.f_fb_other,
  b.f_fb_kid_mty,
  b.f_fb_cosme,
  b.f_fb_mens,
  b.f_fb_smp_app,
  b.f_fb_ent_hby,
  b.f_fb_ticket,
  b.f_fb_interior,
  b.f_fb_handmade,
  b.f_fb_car,
  b.f_fb_food_drink,
  b.f_fb_instr,
  b.f_fb_sports,

  -- sell01
  c.sell_frq,
  c.sell_gms,
  c.sell_avg_price,
  c.sell_avg_discount,
  c.so_frq,
  c.so_gms,
  c.so_avg_price,
  c.so_avg_discount,
  c.so_frq_ratio,
  c.so_gms_ratio,
  c.f_sl_lady,
  c.f_sl_other,
  c.f_sl_kid_mty,
  c.f_sl_cosme,
  c.f_sl_mens,
  c.f_sl_smp_app,
  c.f_sl_ent_hby,
  c.f_sl_ticket,
  c.f_sl_interior,
  c.f_sl_handmade,
  c.f_sl_car,
  c.f_sl_food_drink,
  c.f_sl_instr,
  c.f_sl_sports,

  -- blc01
  d.saving,
  d.sales,
  d.to_r_cash,
  d.out_rtn,
  d.delivery,

  --pd02_cpn_use01
  e.coupon_discount_price,
  e.use_point_num,
  e.use_balance_num,
  e.f_cpn_discount,
  e.f_buy_point,
  e.f_buy_blc,
  e.f_cpn_act,
  e.f_cpn_rate,
  e.f_cpn_passive,
  e.f_cpn_max_pr,
  e.f_cpn_min_pr,
  e.f_cpn_bst,
  e.f_cpn_buy_new,
  e.f_cpn_buy_ext,
  e.f_cpn_buy_oth,
  e.f_cpn_buy_ret,
  e.f_cpn_sell_new,
  e.f_cpn_sell_ret,
  e.f_cpn_sell_oth,

  --pd02_cpn_dist01
  f.cnt_coupon_uq,	
  f.cnt_campaign,	
  f.cnt_exp_term,	
  f.cnt_exp_term_3,	
  f.cnt_exp_term_4,	
  f.cnt_exp_term_7,	
  f.cnt_dct_price,
  f.cnt_discount_price_10,
  f.cnt_discount_price_20,
  f.cnt_discount_price_100,
  f.cnt_discount_price_200,
  f.cnt_discount_price_300,
  f.cnt_discount_price_500,
  f.cnt_discount_price_1000,
  f.cnt_discount_price_2000,
  f.cnt_dct_rate,
  f.cnt_discount_rate_003,
  f.cnt_discount_rate_005,
  f.cnt_min_price,
  f.cnt_min_price_300,
  f.cnt_min_price_500,
  f.cnt_min_price_2000,
  f.cnt_min_price_10000,
  f.cnt_max_price,
  f.cnt_passive,
  
  -------------------- standardized by day
  f.cnt_coupon_uq / (a.date_diff_nn + 1 ) as cnt_coupon_uq_day,
  f.cnt_campaign / (a.date_diff_nn + 1 ) as  cnt_campaign_day,
  f.cnt_exp_term / (a.date_diff_nn + 1 ) as  cnt_exp_term_day,
  f.cnt_exp_term_3 / (a.date_diff_nn + 1 ) as  cnt_exp_term_3_day,
  f.cnt_exp_term_4 / (a.date_diff_nn + 1 ) as  cnt_exp_term_4_day,
  f.cnt_exp_term_7 / (a.date_diff_nn + 1 ) as  cnt_exp_term_7_day,
  f.cnt_dct_price / (a.date_diff_nn + 1 ) as  cnt_dct_price_day,
  f.cnt_discount_price_10 / (a.date_diff_nn + 1 ) as  cnt_discount_price_10_day,
  f.cnt_discount_price_20 / (a.date_diff_nn + 1 ) as  cnt_discount_price_20_day,
  f.cnt_discount_price_100 / (a.date_diff_nn + 1 ) as  cnt_discount_price_100_day,
  f.cnt_discount_price_200 / (a.date_diff_nn + 1 ) as  cnt_discount_price_200_day,
  f.cnt_discount_price_300 / (a.date_diff_nn + 1 ) as  cnt_discount_price_300_day,
  f.cnt_discount_price_500 / (a.date_diff_nn + 1 ) as  cnt_discount_price_500_day,
  f.cnt_discount_price_1000 / (a.date_diff_nn + 1 ) as  cnt_discount_price_1000_day,
  f.cnt_discount_price_2000 / (a.date_diff_nn + 1 ) as  cnt_discount_price_2000_day,
  f.cnt_dct_rate / (a.date_diff_nn + 1 ) as  cnt_dct_rate_day,
  f.cnt_discount_rate_003 / (a.date_diff_nn + 1 ) as  cnt_discount_rate_003_day,
  f.cnt_discount_rate_005 / (a.date_diff_nn + 1 ) as  cnt_discount_rate_005_day,
  f.cnt_min_price / (a.date_diff_nn + 1 ) as  cnt_min_price_day,
  f.cnt_min_price_300 / (a.date_diff_nn + 1 ) as  cnt_min_price_300_day,
  f.cnt_min_price_500 / (a.date_diff_nn + 1 ) as  cnt_min_price_500_day,
  f.cnt_min_price_2000 / (a.date_diff_nn + 1 ) as  cnt_min_price_2000_day,
  f.cnt_min_price_10000 / (a.date_diff_nn + 1 ) as  cnt_min_price_10000_day,
  f.cnt_max_price / (a.date_diff_nn + 1 ) as  cnt_max_price_day,
  f.cnt_passive / (a.date_diff_nn + 1 ) as  cnt_passive_day,

  --pd02_msg01
  g.cnt_msg_item,
  g.cnt_msg_like,
  g.cnt_msg_comment,
  g.cnt_msg_follower,
  g.cnt_msg_discount,
  g.cnt_msg_coupon,
  g.cnt_msg_to_r_cash,
  g.cnt_msg_invitation,
  g.cnt_msg_welcome,
  g.cnt_msg_new_seller,
  g.cnt_msg_survey,
  g.cnt_msg_ad,
  g.cnt_msg_pr,
  g.cnt_msg_rakuma_msg,
  g.cnt_msg_cancel,
  g.cnt_msg_coupon_expiration,
  g.cnt_msg_refund,
  g.cnt_msg_other,

  -------------------- standardized by day
  g.cnt_msg_item / (a.date_diff_nn + 1 ) as  cnt_msg_item_day,
  g.cnt_msg_like / (a.date_diff_nn + 1 ) as  cnt_msg_like_day,
  g.cnt_msg_comment / (a.date_diff_nn + 1 ) as  cnt_msg_comment_day,
  g.cnt_msg_follower / (a.date_diff_nn + 1 ) as  cnt_msg_follower_day,
  g.cnt_msg_discount / (a.date_diff_nn + 1 ) as  cnt_msg_discount_day,
  g.cnt_msg_coupon / (a.date_diff_nn + 1 ) as  cnt_msg_coupon_day,
  g.cnt_msg_to_r_cash / (a.date_diff_nn + 1 ) as  cnt_msg_to_r_cash_day,
  g.cnt_msg_invitation / (a.date_diff_nn + 1 ) as  cnt_msg_invitation_day,
  g.cnt_msg_welcome / (a.date_diff_nn + 1 ) as  cnt_msg_welcome_day,
  g.cnt_msg_new_seller / (a.date_diff_nn + 1 ) as  cnt_msg_new_seller_day,
  g.cnt_msg_survey / (a.date_diff_nn + 1 ) as  cnt_msg_survey_day,
  g.cnt_msg_ad / (a.date_diff_nn + 1 ) as  cnt_msg_ad_day,
  g.cnt_msg_pr / (a.date_diff_nn + 1 ) as  cnt_msg_pr_day,
  g.cnt_msg_rakuma_msg / (a.date_diff_nn + 1 ) as  cnt_msg_rakuma_msg_day,
  g.cnt_msg_cancel / (a.date_diff_nn + 1 ) as  cnt_msg_cancel_day,
  g.cnt_msg_coupon_expiration / (a.date_diff_nn + 1 ) as  cnt_msg_coupon_expiration_day,
  g.cnt_msg_refund / (a.date_diff_nn + 1 ) as  cnt_msg_refund_day,
  g.cnt_msg_other / (a.date_diff_nn + 1 ) as  cnt_msg_other_day,

  --pd02_sc_qry01
  h.f_txt,
  h.f_status,
  h.f_brand,
  h.f_parent_category,
  h.f_category,
  h.f_color,
  h.f_size,
  h.f_size_group,
  h.f_min_sell_price,
  h.f_max_sell_price,
  h.f_tran_status,

  --pd02_rsp_rc01
  i.point,
  i.r_point,
  i.f_point,

  --pd01_rtn01
  j.f_rtn,

  --pd01_inflow01
  k.f_inflow,

  --pd02_item_view01
  l.cnt_ssn,
  l.cnt_item,
  l.cnt_ssn_web,
  l.ttl_dur_min,
  l.ttl_leave_hour,

  -------------------- standardized by day
  l.cnt_ssn / (a.date_diff_nn + 1 ) as cnt_ssn_day,
  l.cnt_item / (a.date_diff_nn + 1 ) as cnt_item_day,
  l.cnt_ssn_web / (a.date_diff_nn + 1 ) as cnt_ssn_web_day,

  --pd02_item_like01
  m.cnt_like_item,
  m.cnt_like_total,

  -------------------- standardized by day
  m.cnt_like_item / (a.date_diff_nn + 1 ) as cnt_like_item_day,
  m.cnt_like_total / (a.date_diff_nn + 1 ) as cnt_like_total_day,

  --pd02_karte01
  n.cnt_karte_uq,
  n.cnt_karte,

  -------------------- standardized by day
  n.cnt_karte_uq / (a.date_diff_nn + 1 ) as cnt_karte_uq_day,
  n.cnt_karte / (a.date_diff_nn + 1 ) as cnt_karte_day,

  --row_number
  row_number() over(order by a.order_user_id) as row_num,

  -------------------- Use coupon at repeat purchase
  o.f_not_cpn 

from
  fril-analysis-staging-165612.ueno_analysis02.pd02_tgt01       as a
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_buy_demo01  as b on a.order_user_id = b.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_sell01      as c on a.order_user_id = c.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_blc01       as d on a.order_user_id = d.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_use01   as e on a.order_user_id = e.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_dist01  as f on a.order_user_id = f.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_msg01       as g on a.order_user_id = g.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_sc_qry01      as h on a.order_user_id = h.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_rsp_rc01    as i on a.order_user_id = i.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd01_rtn01       as j on a.order_user_id = j.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd01_inflow01    as k on a.order_user_id = k.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_item_view01 as l on a.order_user_id = l.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_item_like01 as m on a.order_user_id = m.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_karte01     as n on a.order_user_id = n.order_user_id
inner join
  fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01  as o on a.order_user_id = o.order_user_id
order by 1
;


select * from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_use01 where coupon_discount_price > 0 limit 100;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_model01 limit 10;
select * from fril-analysis-staging-165612.ueno_analysis02.pd02_cpn_buy_rep01  limit 10;

select f_not_cpn, count(distinct order_user_id)  from fril-analysis-staging-165612.ueno_analysis02.pd02_model01 group by rollup(1);
select min(order_user_id), max(order_user_id)  from fril-analysis-staging-165612.ueno_analysis02.pd02_model01 where row_num between 1 and 8000;