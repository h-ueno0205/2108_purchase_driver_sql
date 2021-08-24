
# 目的
# pd03.Rで大まかにあたりをつけた変数で、モデリングをおこなう
# 

# Excel summary
# C:\work\05_analysis\210602_購買ドライバー\02_result\
# 
# 
#
#

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
rm(con)
con <- dbConnect(
  bigrquery::bigquery(),   #db driver
  project = "fril-analysis-staging-165612",
  dataset = "ueno_analysis02",
  page_size = 1000
)

rm(pd01_ana01)
r_cnt <- 20000
r_start <- 1
r_end <- 20000
max_row <- 800000
sql01 <- "select * from pd02_model01"

while (r_end <= max_row){
  
  sql02 <- paste(" where row_num between ", r_start, " and ", r_end, sep="")
  sql_str <- paste(sql01, sql02, sep="")
  print(paste("row_end : ", r_end))
  
  if(r_start == 1){
    pd01_ana01 <- dbGetQuery(con, sql_str)
    print(paste("max : ", max(pd01_ana01$order_user_id)))
    print(paste("max : ", min(pd01_ana01$order_user_id)))
  } else{
    pd01_ana_tmp01 <- dbGetQuery(con, sql_str)
    pd01_ana01 <- bind_rows(pd01_ana01, pd01_ana_tmp01)
    print(paste("max : ", max(pd01_ana_tmp01$order_user_id)))
    print(paste("max : ", min(pd01_ana_tmp01$order_user_id)))
    rm(pd01_ana_tmp01)
  }
  
  r_start <- r_start + r_cnt
  r_end <- r_end + r_cnt 
  gc();gc()
  
}

#tmp01 <- pd01_ana01 %>% group_by(f_not_cpn) %>% summarize(n=n())


#---------------------------------------------------------------- [3] Data process for modeling

#rm(remove)
add_var <- c(
  #order_user_id,f_tgt
  1,4,
  
  #分析変数
  8,9,10,11,12,13,14,15,53,55,57,73,75,76,
  125,126,127,129,130,131,132,133,134,135,138,139,141,
  165,166,167,168,169,170,171,174,175,176,177,178,183,
  184,185,187,188,190,191,192,196,197,202,203,204,205,
  206,209,214
)

#rm(tmp_model01)
tmp_model01 <- pd01_ana01[,add_var]
col_name <- colnames(tmp_model01) 

#---------------------------------------------------------------- [4] Modeling

train_size01 <- 600000
test_size01 <- 100000


# Data frame for summary
est01 <- data.frame(num=0, var='null', coef=0, sd_error=0, z_val=0, prob=0)
fit_stat01 <- data.frame(num=0 ,var ='null', data = 'null', val=0)
act_prob01 <- data.frame(num=0 ,var ='null', train_test = 'null', est_prob = 0, n = 0, tgt = 0, act_prob = 0)


n <- ncol(tmp_model01)
tmp_model02 <- tmp_model01[,c(1:n)]
model_train01 <- sample_n(tmp_model02, size=train_size01)
model_test01  <- sample_n(tmp_model02, size=test_size01)
#nrow(model_test01)

out_logit01   <- glm(f_tgt ~., data = model_train01[-1], family=binomial(link='logit'))
out_logit02   <- summary(out_logit01)
tmp_predict01 <- predict(out_logit01, newdata=model_test01[-1], type="response")


# AUC at Train and Test
print('------------ 3. AUC')

options(digits=2)
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

roc(
    out_predict01$tgt, out_predict01$fv_pred, 
    levels = c(0, 1), 
    direction = "<", 
    smoothed = TRUE,
    # arguments for ci
    ci=TRUE, ci.alpha=0.9, stratified=FALSE,
    # arguments for plot
    plot=TRUE, 
    auc.polygon=TRUE, 
    max.auc.polygon=TRUE, 
    print.auc=TRUE, 
    show.thres=TRUE,
    legacy.axes = TRUE
    )


# Act prob against estimation at Train and Test 
print('------------ 4. Act vs. Pred')

tmp_act_prob_train01 <- out_fitted01  %>% group_by(num, var, train_test, est_prob=round(fv_pred,digit=2)) %>% summarise(n=n(), tgt=sum(tgt), act_prob=sum(tgt)/n())
tmp_act_prob_test01  <-  out_predict01 %>% group_by(num, var, train_test, est_prob=round(fv_pred,digit=2)) %>% summarise(n=n(), tgt=sum(tgt), act_prob=sum(tgt)/n())
act_prob01 <- rbind(act_prob01, tmp_act_prob_train01, tmp_act_prob_test01)


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


# csv
write.csv(act_prob01, "/home/ueno01_r/2106_purchase_drivers/01_act_prob01.csv")
write.csv(est01, "/home/ueno01_r/2106_purchase_drivers/02_est01.csv")
write.csv(fit_stat01, "/home/ueno01_r/2106_purchase_drivers/03_fit_stat01.csv")

saveRDS(
          object = out_logit01,
          file = paste("/home/ueno01_r/2106_purchase_drivers/object/", "out_logit01_prodction.obj",sep="")
        )


#---------------------------------------------------------------- [5] Model selection

tm <- as.POSIXlt(Sys.time()+(60*60*9), "JST") 
print(paste("time:", tm))
out_step <- step(out_logit01)

tmp_predict02 <- predict(out_step, newdata=out_step$model, type="response")
tmp01 <- data.frame(fv_pred=tmp_predict02, tgt=out_step$y) 
tmp_act_prob_test02  <- tmp01 %>% 
                                group_by(est_prob=round(fv_pred,digit=2)) %>% 
                                summarise(n=n(), tgt=sum(tgt), act_prob=sum(tgt)/n())
roc_test  <- roc(
                  out_step$y, tmp_predict02,
                  smoothed = TRUE,
                  # arguments for ci
                  ci=TRUE, ci.alpha=0.9, stratified=FALSE,
                  # arguments for plot
                  plot=TRUE, 
                  auc.polygon=TRUE, 
                  max.auc.polygon=TRUE, 
                  print.auc=TRUE, 
                  show.thres=TRUE,
                  legacy.axes = TRUE,
                  levels = c(0, 1), 
                  direction = "<"
                )


saveRDS(
  object = out_step,
  file = paste("/home/ueno01_r/2106_purchase_drivers/object/", "out_step_prod2.obj",sep="")
)

data.frame(coef=out_step$coefficients)

