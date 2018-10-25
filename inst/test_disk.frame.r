library(data.table)
library(future)
library(future.apply)
library(fst)
library(disk.frame)
plan(multiprocess)

source("R/disk.frame.r")
source("R/merge.disk.frame.r")
source("R/hard_group_by.r")

a = disk.frame("c:/users/l098905/test_hl/")

hard_group_by.disk.frame(a,"ACCOUNT_ID",outdir="c:/users/l098905/test_hl_grouped/")


b = disk.frame("c:/users/l098905/test_hl_grouped2/")
d = disk.frame("c:/users/l098905/test_hl_grouped/")

merge.disk.frame(b,d,by=c("ACCOUNT_ID","MONTH_KEY"),outdir="C:/Users/L098905/test_hl_grouped3")

dd1 = read_fst("c:/users/l098905/test_hl_grouped2/1.fst",as.data.table = T)
dd2 = read_fst("c:/users/l098905/test_hl_grouped/1.fst",as.data.table = T)


dd3 = merge(dd1,dd2,by=)

b1 = keep.disk.frame(b,c("ACCOUNT_ID","MONTH_KEY"))
d1 = keep.disk.frame(d,c("ACCOUNT_ID","MONTH_KEY"))


db1 = merge(b1,d1,by=c("ACCOUNT_ID","MONTH_KEY"),outdir="C:/Users/L098905/test_hl_grouped3")

db2 = merge(b1,d1,by=c("ACCOUNT_ID","MONTH_KEY"),outdir="C:/Users/L098905/test_hl_grouped3", merge_by_chunk_id = T)