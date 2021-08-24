# Excel summary
# C:\work\05_analysis\210602_購買ドライバー\02_result\210729_logit01.xlsx
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


#---------------------------------------------------------------- [1] load data
# Connect and extract data from BQ
con <- dbConnect(
  bigrquery::bigquery(),   #db driver
  project = "fril-analysis-staging-165612",
  dataset = "ueno_analysis02",
  page_size = 10000
)

rm(pd01_ana01)
sql <- "select * from pd02_model01 order by 1 "
pd01_ana01 <- dbGetQuery(con, sql)
pd01_ana01 <- pd01_ana01[,-164]

# add random
tmp_ran <- data.frame(rand=runif(nrow(pd01_ana01), min=0, max=1))
pd01_ana01 <- cbind(pd01_ana01, tmp_ran)
colnames(pd01_ana01)

#---------------------------------------------------------------- [2] Stats
basic_stats <- describe(pd01_ana01)
write.csv(basic_stats, "/home/ueno01_r/2106_purchase_drivers/basic_stats01.csv")

cor_all <- cor(pd01_ana01[,-2])
write.csv(cor_all, "/home/ueno01_r/2106_purchase_drivers/cor_matrix01.csv")


# flattening cor_all
cnt_col <- 1; cnt_row <- 1
row_name <- rownames(cor_all); col_name <- colnames(cor_all)
cor_flat <- data.frame(row_num=0, col_num=0, row='dummy', col='dummy', cor=0)

while(cnt_row<=length(row_name)){
  while (cnt_col < cnt_row ) {
    tmp <- data.frame(
      row_num=cnt_row, 
      col_num=cnt_col,
      row=row_name[cnt_row],
      col=col_name[cnt_col],
      cor=cor_all[cnt_row, cnt_col]
    )
    cor_flat <- rbind(cor_flat, tmp)
    cnt_col <- cnt_col + 1
  }
  cnt_row <- cnt_row + 1 
  cnt_col <- 1
  #print(cnt_row)
}

write.csv(cor_flat, "/home/ueno01_r/2106_purchase_drivers/cor_flat01.csv")





#---------------------------------------------------------------- [3] Data process for modeling
model01 <- subset(pd01_ana01, tmp_ran < 0.1)
nrow(model01)
remove <- c(
              ##N/A order_user_id, f_ana01,,date_diff 
              -1,-2,-3,
              #NA 相関係数計算できない（データ不備）
              -7,-28, -52,-56,-87,-120,-130,-143,-151,
              #多重共線性
              -54,-58,-74,-80,-81,-85,-88,-97,-98,-99,-103,-112,-115,-146,-152,-161,-162,
              #対象者がすくない
              -16,-34,-47,-69,-71,-83,-91,-93,-94,-95,-96,-111,-117,-118,-119,-129,-138,-139,
              #random
              -164
)
model01 <- model01[,remove]
ncol(model01)

#---------------------------------------------------------------- [3] Logistic regression
out_logit01 <- glm(f_tgt ~., data = model01, family=binomial(link='logit'))
out_summary01 <- summary(out_logit01)

# check fitted value
chk_fit <- data.frame(
                        fv = out_logit01$fitted.values,
                        rnd_fv=round(out_logit01$fitted.values,digit=2),
                        tgt=model01$f_tgt
                      )
out_fit <- group_by(chk_fit,rnd_fv) %>% summarise(n=n(), tgt=sum(tgt))


# Draw ROC and Calc AUC
roc_obj <- roc(
               chk_fit$tgt, chk_fit$fv,
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








