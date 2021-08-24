# Check Describe stats and Correlation
##
# Excel summary
# C:\work\05_analysis\210602_購買ドライバー\02_result\
#
#
#

# Library
library("psych")
library("bigrquery")
library("DBI")

# Settings
setwd("/home/ueno01_r/2106_purchase_drivers")
getwd()
options(dplyr.summarise.inform = FALSE)


#---------------------------------------------------------------- [2] Stats
basic_stats <- describe(pd01_ana01)
write.csv(basic_stats, "/home/ueno01_r/2106_purchase_drivers/basic_stats01.csv")

cor_all <- cor(pd01_ana01[,c(-2,-216)])
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
rm(cor_all); rm(cor_flat); rm(basic_stats)


