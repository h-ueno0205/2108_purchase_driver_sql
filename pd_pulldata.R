

# 目的
# BigqueryからRにデータをもってくる
# 


# Library
library("bigrquery")
library("DBI")
library("dplyr")
library("psych")


# Settings
setwd("/home/ueno01_r/2106_purchase_drivers")
getwd()
options(dplyr.summarise.inform = FALSE)
options(digits=22)

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

# coupon利用者を除外
pd01_ana01_copy <- pd01_ana01
pd01_ana01 <- subset(pd01_ana01, f_not_cpn != '03_cpn')


# Data CHK
nrow(pd01_ana01)
basic_stats <- describe(pd01_ana01)
write.csv(basic_stats, "/home/ueno01_r/2106_purchase_drivers/basic_stats01.csv")
pd01_ana01 %>% group_by(f_not_cpn) %>% summarize(n=n())

sum(pd01_ana01$order_user_id)


