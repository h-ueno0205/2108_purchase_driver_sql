
# 目的
# Step関数が動かないので、変数を1個づつ追加した時の精度を確認して
# 削除する変数の目星をつける




# Excel summary
# C:\work\05_analysis\210602_購買ドライバー\02_result\
# 210730_logit01.xlsx
# 210802_logit01_02.xlsx
#
#

# Pakage 
install.packages("DBI")
install.packages("bigrquery")


# Library
#library("reshape2")
library("dplyr")
library("data.table")
library("bigrquery")
library("DBI")
library("psych")
library("pROC")

# Settings
setwd("/home/ueno01_r/2106_purchase_drivers")
getwd()
options(dplyr.summarise.inform = FALSE)



#---------------------------------------------------------------- [1] load data
# Connect and extract data from BQ
con <- dbConnect(
  bigrquery::bigquery(),   #db driver
  project = "fril-analysis-staging-165612",
  dataset = "ueno_analysis02",
  page_size = 8000
)

rm(pd01_ana01)
r_cnt <- 8000
r_start <- 1
r_end <- 8000
max_row <- 800000
sql01 <- "select * from pd02_model01"

while (r_end <= max_row){

  sql02 <- paste(" where row_num between ", r_start, " and ", r_end, sep="")
  sql_str <- paste(sql01, sql02, sep="")
  print(paste("row_end : ", r_end))

  if(r_start == 1){
      pd01_ana01 <- dbGetQuery(con, sql_str)
  } else{
    pd01_ana_tmp01 <- dbGetQuery(con, sql_str)
    pd01_ana01 <- bind_rows(pd01_ana01, pd01_ana_tmp01)
    rm(pd01_ana_tmp01)
  }
  
  r_start <- r_start + r_cnt
  r_end <- r_end + r_cnt 

}


#nrow(pd01_ana01); max(pd01_ana01$order_user_id); sum(pd01_ana01$order_user_id); gc();gc()
#add random
#tmp_ran <- data.frame(rand=runif(nrow(pd01_ana01), min=0, max=1))
#pd01_ana01 <- cbind(pd01_ana01, tmp_ran)
#colnames(pd01_ana01)

#pd01_ana01 %>% group_by(f_not_cpn) %>% summarise(n=n())
#col_name <- colnames(pd01_ana01) 


#---------------------------------------------------------------- [3] Data process for modeling
remove <- c(
  # f_ana01,date_diff 
  -2,-3,
  #NA 相関係数計算できない（データ不備）
  -7,-28,-52,-56,-87,-120,-155,-186,-194,


  #多重共線性
  -54,-58,-74,-80,-81,-85,-88,-97,-98,-99,-100,-101,-102,-103,-104,-105,-106,-107,
  -108,-109,-110,-112,-113,-114,-115,-116,-121,-122,-123,-124,-128,-136,-137,-140,
  -142,-143,-144,-145,-147,-148,-149,-150,-151,-152,-153,-156,-157,-158,-159,-160,
  -161,-162,-172,-173,-181,-182,-189,-195,-208,-211,
  
  
  -54,-58,-74,-80,-81,-85,-88,-97,-98,-99,-100,-101,-102,-103,-104,-105,-106,-107,
  -108,-109,-110,-112,-113,-114,-115,-116,-121,-122,-123,-124,-128,-136,-137,-140,
  -142,-143,-144,-145,-147,-148,-149,-150,-151,-152,-153,-156,-157,-158,-159,-160,
  -161,-162,-172,-173,-181,-182,-189,-195,-199,-200,-201,-207,-208,-210,-211,-212,
  -213,-146,

  #対象者がすくない
  -16,-34,-47,-69,-71,-83,-91,-93,-94,-95,-96,-111,-117,-118,-119,-154,-163,-164,

  #行番号
  -215, -216
)

#rm(tmp_model01); rm(col_name)
tmp_model01 <- pd01_ana01[,remove]
col_name <- colnames(tmp_model01) 

#---------------------------------------------------------------- [4] Modeling

# var_settings
n <- 5 # 5 : f_fb_pr_1k
train_size01 <- 200000
test_size01 <- 100000

# Data frame for summary
est01 <- data.frame(num=0, var='null', coef=0, sd_error=0, z_val=0, prob=0)
fit_stat01 <- data.frame(num=0 ,var ='null', data = 'null', val=0)
act_prob01 <- data.frame(num=0 ,var ='null', train_test = 'null', est_prob = 0, n = 0, tgt = 0, act_prob = 0)
#id_fit_pred01 <- data.frame(num=0,var=0,train_test='dummy',order_user_id=0,fv_pred=0,tgt=0)

#remove
#rm(act_prob01); rm(est01); rm(fit_stat01); rm(id_fit_pred01)


#n <- 117

while (n<=ncol(tmp_model01)){
#while (n<=10){
  
  tm <- as.POSIXlt(Sys.time()+(60*60*9), "JST") 
  print(paste("time:", tm, " / model:", n, ' / var:', col_name[n], sep=""))
  print('------------ 1. sampling')
  
  # Sampling
  tmp_model02 <- tmp_model01[,c(1:n)]
  model_train01 <- sample_n(tmp_model02, size=train_size01)
  model_test01  <- sample_n(tmp_model02, size=test_size01)

  # Modeling and Prediction
  print('------------ 2. modeling and prediction')
  
  out_logit01   <- glm(f_tgt ~., data = model_train01[-1], family=binomial(link='logit'))
  out_logit02   <- summary(out_logit01)
  tmp_predict01 <- predict(out_logit01, newdata=model_test01[-1], type="response")

  
  # AUC at Train and Test
  print('------------ 3. AUC')
  out_fitted01  <- data.frame(
                                num = n ,
                                var = col_name[n], 
                                train_test = 'train', 
                                order_user_id = model_train01$order_user_id,
                                fv_pred=out_logit01$fitted.values, 
                                tgt=model_train01$f_tgt
                              )
  out_predict01 <- data.frame(
                                num = n ,
                                var = col_name[n], 
                                train_test = 'test',
                                order_user_id = model_test01$order_user_id,
                                fv_pred=tmp_predict01, 
                                tgt=model_test01$f_tgt
                              )
  roc_train <- roc(out_fitted01$tgt, out_fitted01$fv_pred,   levels = c(0, 1), direction = "<")
  roc_test  <- roc(out_predict01$tgt, out_predict01$fv_pred, levels = c(0, 1), direction = "<")
  
  # Act prob against estimation at Train and Test 
  print('------------ 4. Act vs. Pred')
  
  tmp_act_prob_train01 <- out_fitted01  %>% group_by(num, var, train_test, est_prob=round(fv_pred,digit=2)) %>% summarise(n=n(), tgt=sum(tgt), act_prob=sum(tgt)/n())
  tmp_act_prob_test01  <-  out_predict01 %>% group_by(num, var, train_test, est_prob=round(fv_pred,digit=2)) %>% summarise(n=n(), tgt=sum(tgt), act_prob=sum(tgt)/n())

  act_prob01 <- rbind(act_prob01, tmp_act_prob_train01, tmp_act_prob_test01)
  
  
  # Summarize modeling results
  print('------------ 5. Summarize')
  
  # for est01
  tmp_est01        <- data.frame(num = n ,var = col_name[n], val = out_logit02$coefficients)
  colnames(tmp_est01) <- c('num', 'var', 'coef', 'sd_error', 'z_val', 'prob')
  est01 <- rbind(est01, tmp_est01)

  # for fit01
  tmp_auc_train01   <- data.frame(num = n ,var = col_name[n], data = 'auc_train', val = roc_train$auc[1])
  tmp_auc_test01    <- data.frame(num = n ,var = col_name[n], data = 'auc_test',  val = roc_test$auc[1])
  tmp_aic_train01   <- data.frame(num = n ,var = col_name[n], data = 'aic_test',  val = out_logit01$aic)
  tmp_nul_dev_test01    <- data.frame(num = n ,var = col_name[n], data = 'null_dev',  val = out_logit01$null.deviance)
  tmp_res_dev_test01    <- data.frame(num = n ,var = col_name[n], data = 'res_dev',  val = out_logit01$deviance)
  
  fit_stat01 <- rbind (fit_stat01, tmp_auc_train01, tmp_auc_test01, tmp_aic_train01, tmp_nul_dev_test01, tmp_res_dev_test01)

  
  # for id_fit_pred01
  #id_fit_pred01 <- rbind(id_fit_pred01, out_fitted01, out_predict01)

  # Save files and objects
  print('------------ 6. Save modeling object')
  #saveRDS(
  #          object = out_logit01,
  #          file = paste("/home/ueno01_r/2106_purchase_drivers/object/", n, "_",col_name[n], "_logit.obj",sep="")
  #        )

  # counter
  n <- n + 1

  # remove objects and garbage collection
  rm(tmp_model02); rm(model_train01); rm(model_test01)
  rm(model_train01); rm(model_test01)
  rm(out_logit01); rm(out_logit02); rm(tmp_predict01)
  rm(out_fitted01);rm(out_predict01);rm(roc_train);rm(roc_test)
  rm(tmp_act_prob_train01); rm(tmp_act_prob_test01)
  rm(tmp_est01)
  rm(tmp_auc_train01);rm(tmp_auc_test01);rm(tmp_aic_train01);rm(tmp_nul_dev_test01);rm(tmp_res_dev_test01)
  rm(tmp01)
  gc();gc()
  
}

# csv
write.csv(act_prob01, "/home/ueno01_r/2106_purchase_drivers/01_act_prob01.csv")
write.csv(est01, "/home/ueno01_r/2106_purchase_drivers/02_est01.csv")
write.csv(fit_stat01, "/home/ueno01_r/2106_purchase_drivers/03_fit_stat01.csv")
#write.csv(id_fit_pred01, "/home/ueno01_r/2106_purchase_drivers/04_id_fit_pred01.csv")



