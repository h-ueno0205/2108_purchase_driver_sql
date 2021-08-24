# Excel summary
# C:\work\05_analysis\210602_購買ドライバー\02_result\210729_logit_test01.xlsx
#
#
#




#------------------------------ Purchase drivers
# Install packages
install.packages("bigrquery")
install.packages("DBI")
install.packages("psych")
install.packages("Epi")
install.packages("pROC")


# Library
#library("reshape2")
library("dplyr")
library("sqldf")
library("data.table")
library("parallel")
library("bigrquery")
library("DBI")
library("psych")
library("pROC")

# Settings
setwd("/home/ueno01_r/2106_purchase_drivers")
getwd()

#---------------------------------------------------------------- [1] load raw data and process data
# Connect and extract data from BQ
con <- dbConnect(
  bigrquery::bigquery(),
  project = "fril-analysis-staging-165612",
  dataset = "ueno_analysis02",
)

rm(pd01_ana01)
nrow(pd01_ana01)
sql <- "select * from pd02_model01 order by 1"
pd01_ana01 <- dbGetQuery(con, sql)




# basic stats
basic_stats <- describe(pd01_ana01)
cor_all <- cor(pd01_ana01[,-2])
write.csv(basic_stats, "/home/ueno01_r/2106_purchase_drivers/basic_stats01.csv")

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


# add random
tmp_ran <- data.frame(rand=runif(nrow(pd01_ana01), min=0, max=1))
pd01_ana01 <- cbind(pd01_ana01, tmp_ran)

# --- data for model
test_model01 <- subset(pd01_ana01, tmp_ran < 0.1)
nrow(test_model01)
remove <- c(
              #order_user_id, f_ana01,,date_diff 
              -1,-2,-3,
              #NULL
              -7, -52,-56,-87,-120,-130,-143,-151,
              # smaller samples
              -16,-28,-34,-47,-48,-69,-71,-80,-83,-85,-88,-91,-93,-94,-95,-96,
              -102,-103,-112,-114,-115, -117, -118,-119,
              # rand
              -164
            )
model01 <- test_model01[,remove]
nrow(model01)



#---------------------------------------------------------------- [2] Logistic regression
out_logit01 <- glm(f_tgt ~., data = model01, family=binomial(link='logit'))
out_summary01 <- summary(out_logit01)

# check fitted value
a <-data.frame(fv=out_logit01$fitted.values,rnd_fv=round(out_logit01$fitted.values,digit=2),tgt=model01$f_tgt)
out_fit <- group_by(a,rnd_fv) %>% summarise(n=n(), tgt=sum(tgt))


# Draw ROC and Calc AUC
data_roc <- data.frame(fv=out_logit01$fitted.values, tgt=model01$f_tgt)
roc_obj <- roc(data_roc$tgt, data_roc$fv,
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


# Model selection --- > 計算が終わらない
out_step <- step(out_logit01)













#--------------------------------------------------------------------------------------- epi ROC function AUC計算確認

#ROC logic check epi package
lr <- glm( data_roc$tgt ~ data_roc$fv, family=binomial )
resp <- data_roc$tgt
PS <- is.null(data_roc$fv)


#予測確率ごとの目的変数度数分布を作成
tmp_m <- switch( PS+1, data_roc$fv, lr$fit)   # PS=NOT NULL→Falseのとき1になってdata_roc$fv / Trueのとき2でlr$fit
tmp_m2 <- as.matrix(table(tmp_m,resp))  #予測確率ごとのtgtの度数
m  <- addmargins( rbind( 0, tmp_m2 ), 2 ) #tgt周辺度数(列と行のtgt合計)
nr <- nrow(m)
m <- apply( m, 2, cumsum ) # 予測値が小さい方からのtgt合計値
fv <- c(#-Inf, 
  sort( unique(switch( PS+1, data_roc$fv, lr$fit ) ) ) ) # 予測値のユニーク

#予測確率ごとの感度？・特異度？的なものを計算(packageはsnsとspcの式が逆になっているっぽい)
sns <- (m[nr,2]-m[,2]) /   m[nr,2]
spc <-          m[,1]  /   m[nr,1]   #予測確率ベースの陰性構成比
pvp <-          m[,2]  /           m[,3]     #予測確率ごとの陽性率
pvn <- (m[nr,1]-m[,1]) / ( m[nr,3]-m[,3] )
res <- data.frame(cbind(sns, spc, pvp, pvn, fv))
rnam <- deparse(substitute(data_roc$tgt))
names( res ) <- c( "sens", "spec", "pvp", "pvn", rnam )

# AUC計算→snsとspcの式を逆にすると、たぶん、正しい値がでてくる
# AUC by triangulation
auc <- sum((res[-1,"sens"]+res[-nr,"sens"])/2 * abs(diff(1-res[,"spec"])) )

#--------------------------------------------------------------------------------------- epi ROC function AUC計算確認






