select
  sum(order_user_id) as order_user_id,
  "f_ana01",
  sum(date_diff_nn) as date_diff_nn,
  sum(f_tgt) as f_tgt,
  sum(f_male) as f_male,
  sum(f_female) as f_female,
  sum(f_gender_null) as f_gender_null,
  sum(f_fb_pr_1k) as f_fb_pr_1k,
  sum(f_fb_pr_3k) as f_fb_pr_3k,
  sum(f_fb_pr_5k) as f_fb_pr_5k,
  sum(f_fb_pr_10k) as f_fb_pr_10k,
  sum(f_fb_pr_25k) as f_fb_pr_25k,
  sum(f_fb_pr_50k) as f_fb_pr_50k,
  sum(f_fb_pr_100k) as f_fb_pr_100k,
  sum(f_fb_pr_200k) as f_fb_pr_200k,
  sum(f_fb_pr_201k) as f_fb_pr_201k,
  sum(f_clst_fb_a) as f_clst_fb_a,
  sum(f_clst_fb_b) as f_clst_fb_b,
  sum(f_clst_fb_c) as f_clst_fb_c,
  sum(f_clst_fb_d) as f_clst_fb_d,
  sum(f_clst_fb_e) as f_clst_fb_e,
  sum(f_clst_fb_f) as f_clst_fb_f,
  sum(f_clst_fb_g) as f_clst_fb_g,
  sum(f_clst_fb_h) as f_clst_fb_h,
  sum(f_clst_fb_i) as f_clst_fb_i,
  sum(f_clst_fb_j) as f_clst_fb_j,
  sum(f_clst_fb_k) as f_clst_fb_k,
  sum(f_clst_fb_l) as f_clst_fb_l,
  sum(f_age_null) as f_age_null,
  sum(f_age_20) as f_age_20,
  sum(f_age_30) as f_age_30,
  sum(f_age_40) as f_age_40,
  sum(f_age_50) as f_age_50,
  sum(f_age_60) as f_age_60,
  sum(f_fb_lady) as f_fb_lady,
  sum(f_fb_other) as f_fb_other,
  sum(f_fb_kid_mty) as f_fb_kid_mty,
  sum(f_fb_cosme) as f_fb_cosme,
  sum(f_fb_mens) as f_fb_mens,
  sum(f_fb_smp_app) as f_fb_smp_app,
  sum(f_fb_ent_hby) as f_fb_ent_hby,
  sum(f_fb_ticket) as f_fb_ticket,
  sum(f_fb_interior) as f_fb_interior,
  sum(f_fb_handmade) as f_fb_handmade,
  sum(f_fb_car) as f_fb_car,
  sum(f_fb_food_drink) as f_fb_food_drink,
  sum(f_fb_instr) as f_fb_instr,
  sum(f_fb_sports) as f_fb_sports,
  sum(sell_frq) as sell_frq,
  sum(sell_gms) as sell_gms,
  sum(sell_avg_price) as sell_avg_price,
  sum(sell_avg_discount) as sell_avg_discount,
  sum(so_frq) as so_frq,
  sum(so_gms) as so_gms,
  sum(so_avg_price) as so_avg_price,
  sum(so_avg_discount) as so_avg_discount,
  sum(so_frq_ratio) as so_frq_ratio,
  sum(so_gms_ratio) as so_gms_ratio,
  sum(f_sl_lady) as f_sl_lady,
  sum(f_sl_other) as f_sl_other,
  sum(f_sl_kid_mty) as f_sl_kid_mty,
  sum(f_sl_cosme) as f_sl_cosme,
  sum(f_sl_mens) as f_sl_mens,
  sum(f_sl_smp_app) as f_sl_smp_app,
  sum(f_sl_ent_hby) as f_sl_ent_hby,
  sum(f_sl_ticket) as f_sl_ticket,
  sum(f_sl_interior) as f_sl_interior,
  sum(f_sl_handmade) as f_sl_handmade,
  sum(f_sl_car) as f_sl_car,
  sum(f_sl_food_drink) as f_sl_food_drink,
  sum(f_sl_instr) as f_sl_instr,
  sum(f_sl_sports) as f_sl_sports,
  sum(saving) as saving,
  sum(sales) as sales,
  sum(to_r_cash) as to_r_cash,
  sum(out_rtn) as out_rtn,
  sum(delivery) as delivery,
  sum(coupon_discount_price) as coupon_discount_price,
  sum(use_point_num) as use_point_num,
  sum(use_balance_num) as use_balance_num,
  sum(f_cpn_discount) as f_cpn_discount,
  sum(f_buy_point) as f_buy_point,
  sum(f_buy_blc) as f_buy_blc,
  sum(f_cpn_act) as f_cpn_act,
  sum(f_cpn_rate) as f_cpn_rate,
  sum(f_cpn_passive) as f_cpn_passive,
  sum(f_cpn_max_pr) as f_cpn_max_pr,
  sum(f_cpn_min_pr) as f_cpn_min_pr,
  sum(f_cpn_bst) as f_cpn_bst,
  sum(f_cpn_buy_new) as f_cpn_buy_new,
  sum(f_cpn_buy_ext) as f_cpn_buy_ext,
  sum(f_cpn_buy_oth) as f_cpn_buy_oth,
  sum(f_cpn_buy_ret) as f_cpn_buy_ret,
  sum(f_cpn_sell_new) as f_cpn_sell_new,
  sum(f_cpn_sell_ret) as f_cpn_sell_ret,
  sum(f_cpn_sell_oth) as f_cpn_sell_oth,
  sum(cnt_coupon_uq) as cnt_coupon_uq,
  sum(cnt_campaign) as cnt_campaign,
  sum(cnt_exp_term) as cnt_exp_term,
  sum(cnt_exp_term_3) as cnt_exp_term_3,
  sum(cnt_exp_term_4) as cnt_exp_term_4,
  sum(cnt_exp_term_7) as cnt_exp_term_7,
  sum(cnt_dct_price) as cnt_dct_price,
  sum(cnt_discount_price_10) as cnt_discount_price_10,
  sum(cnt_discount_price_20) as cnt_discount_price_20,
  sum(cnt_discount_price_100) as cnt_discount_price_100,
  sum(cnt_discount_price_200) as cnt_discount_price_200,
  sum(cnt_discount_price_300) as cnt_discount_price_300,
  sum(cnt_discount_price_500) as cnt_discount_price_500,
  sum(cnt_discount_price_1000) as cnt_discount_price_1000,
  sum(cnt_discount_price_2000) as cnt_discount_price_2000,
  sum(cnt_dct_rate) as cnt_dct_rate,
  sum(cnt_discount_rate_003) as cnt_discount_rate_003,
  sum(cnt_discount_rate_005) as cnt_discount_rate_005,
  sum(cnt_min_price) as cnt_min_price,
  sum(cnt_min_price_300) as cnt_min_price_300,
  sum(cnt_min_price_500) as cnt_min_price_500,
  sum(cnt_min_price_2000) as cnt_min_price_2000,
  sum(cnt_min_price_10000) as cnt_min_price_10000,
  sum(cnt_max_price) as cnt_max_price,
  sum(cnt_passive) as cnt_passive,
  sum(cnt_coupon_uq_day) as cnt_coupon_uq_day,
  sum(cnt_campaign_day) as cnt_campaign_day,
  sum(cnt_exp_term_day) as cnt_exp_term_day,
  sum(cnt_exp_term_3_day) as cnt_exp_term_3_day,
  sum(cnt_exp_term_4_day) as cnt_exp_term_4_day,
  sum(cnt_exp_term_7_day) as cnt_exp_term_7_day,
  sum(cnt_dct_price_day) as cnt_dct_price_day,
  sum(cnt_discount_price_10_day) as cnt_discount_price_10_day,
  sum(cnt_discount_price_20_day) as cnt_discount_price_20_day,
  sum(cnt_discount_price_100_day) as cnt_discount_price_100_day,
  sum(cnt_discount_price_200_day) as cnt_discount_price_200_day,
  sum(cnt_discount_price_300_day) as cnt_discount_price_300_day,
  sum(cnt_discount_price_500_day) as cnt_discount_price_500_day,
  sum(cnt_discount_price_1000_day) as cnt_discount_price_1000_day,
  sum(cnt_discount_price_2000_day) as cnt_discount_price_2000_day,
  sum(cnt_dct_rate_day) as cnt_dct_rate_day,
  sum(cnt_discount_rate_003_day) as cnt_discount_rate_003_day,
  sum(cnt_discount_rate_005_day) as cnt_discount_rate_005_day,
  sum(cnt_min_price_day) as cnt_min_price_day,
  sum(cnt_min_price_300_day) as cnt_min_price_300_day,
  sum(cnt_min_price_500_day) as cnt_min_price_500_day,
  sum(cnt_min_price_2000_day) as cnt_min_price_2000_day,
  sum(cnt_min_price_10000_day) as cnt_min_price_10000_day,
  sum(cnt_max_price_day) as cnt_max_price_day,
  sum(cnt_passive_day) as cnt_passive_day,
  sum(cnt_msg_item) as cnt_msg_item,
  sum(cnt_msg_like) as cnt_msg_like,
  sum(cnt_msg_comment) as cnt_msg_comment,
  sum(cnt_msg_follower) as cnt_msg_follower,
  sum(cnt_msg_discount) as cnt_msg_discount,
  sum(cnt_msg_coupon) as cnt_msg_coupon,
  sum(cnt_msg_to_r_cash) as cnt_msg_to_r_cash,
  sum(cnt_msg_invitation) as cnt_msg_invitation,
  sum(cnt_msg_welcome) as cnt_msg_welcome,
  sum(cnt_msg_new_seller) as cnt_msg_new_seller,
  sum(cnt_msg_survey) as cnt_msg_survey,
  sum(cnt_msg_ad) as cnt_msg_ad,
  sum(cnt_msg_pr) as cnt_msg_pr,
  sum(cnt_msg_rakuma_msg) as cnt_msg_rakuma_msg,
  sum(cnt_msg_cancel) as cnt_msg_cancel,
  sum(cnt_msg_coupon_expiration) as cnt_msg_coupon_expiration,
  sum(cnt_msg_refund) as cnt_msg_refund,
  sum(cnt_msg_other) as cnt_msg_other,
  sum(cnt_msg_item_day) as cnt_msg_item_day,
  sum(cnt_msg_like_day) as cnt_msg_like_day,
  sum(cnt_msg_comment_day) as cnt_msg_comment_day,
  sum(cnt_msg_follower_day) as cnt_msg_follower_day,
  sum(cnt_msg_discount_day) as cnt_msg_discount_day,
  sum(cnt_msg_coupon_day) as cnt_msg_coupon_day,
  sum(cnt_msg_to_r_cash_day) as cnt_msg_to_r_cash_day,
  sum(cnt_msg_invitation_day) as cnt_msg_invitation_day,
  sum(cnt_msg_welcome_day) as cnt_msg_welcome_day,
  sum(cnt_msg_new_seller_day) as cnt_msg_new_seller_day,
  sum(cnt_msg_survey_day) as cnt_msg_survey_day,
  sum(cnt_msg_ad_day) as cnt_msg_ad_day,
  sum(cnt_msg_pr_day) as cnt_msg_pr_day,
  sum(cnt_msg_rakuma_msg_day) as cnt_msg_rakuma_msg_day,
  sum(cnt_msg_cancel_day) as cnt_msg_cancel_day,
  sum(cnt_msg_coupon_expiration_day) as cnt_msg_coupon_expiration_day,
  sum(cnt_msg_refund_day) as cnt_msg_refund_day,
  sum(cnt_msg_other_day) as cnt_msg_other_day,
  sum(f_txt) as f_txt,
  sum(f_status) as f_status,
  sum(f_brand) as f_brand,
  sum(f_parent_category) as f_parent_category,
  sum(f_category) as f_category,
  sum(f_color) as f_color,
  sum(f_size) as f_size,
  sum(f_size_group) as f_size_group,
  sum(f_min_sell_price) as f_min_sell_price,
  sum(f_max_sell_price) as f_max_sell_price,
  sum(f_tran_status) as f_tran_status,
  sum(point) as point,
  sum(r_point) as r_point,
  sum(f_point) as f_point,
  sum(f_rtn) as f_rtn,
  sum(f_inflow) as f_inflow,
  sum(cnt_ssn) as cnt_ssn,
  sum(cnt_item) as cnt_item,
  sum(cnt_ssn_web) as cnt_ssn_web,
  sum(ttl_dur_min) as ttl_dur_min,
  sum(ttl_leave_hour) as ttl_leave_hour,
  sum(cnt_ssn_day) as cnt_ssn_day,
  sum(cnt_item_day) as cnt_item_day,
  sum(cnt_ssn_web_day) as cnt_ssn_web_day,
  sum(cnt_like_item) as cnt_like_item,
  sum(cnt_like_total) as cnt_like_total,
  sum(cnt_like_item_day) as cnt_like_item_day,
  sum(cnt_like_total_day) as cnt_like_total_day,
  sum(cnt_karte_uq) as cnt_karte_uq,
  sum(cnt_karte) as cnt_karte,
  sum(cnt_karte_uq_day) as cnt_karte_uq_day,
  sum(cnt_karte_day) as cnt_karte_day,
  sum(row_num) as row_num,
  "f_not_cpn"
from
  fril-analysis-staging-165612.ueno_analysis02.pd02_model01
 ;