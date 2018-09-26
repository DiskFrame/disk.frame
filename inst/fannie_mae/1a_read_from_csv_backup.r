library(fst)
library(data.table)
library(future)
library(future.apply)
library(glue)
library(disk.frame)
library(magrittr)
library(purrr)
library(pryr)
#plan(multiprocess(workers= parallel::detectCores()/2)) # use only half the cores
plan(multiprocess(workers = 3))
#plan(sequential)
source("R/disk.frame.r")
source("R/csv2disk.frame.r")
source("R/hard_group_by.r")
source("R/rbindlist.disk.frame.r")
#source("R/zip_to_disk.frame.r")

raw_perf_data_path = "d:/data/fannie_mae/Performance_All/"

Performance_ColClasses = 
  c("character", "character", "character", "numeric", "numeric", "numeric", "numeric", 
    "numeric", "character", "character", "character", "character", "character", "character", 
    "character", "character", "character", "numeric", "numeric", "numeric", "numeric", "numeric", 
    "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", 
    "character")
Performance_Variables = 
  c("LOAN_ID", "Monthly.Rpt.Prd", "Servicer.Name", "LAST_RT", "LAST_UPB", "Loan.Age", 
    "Months.To.Legal.Mat" , "Adj.Month.To.Mat", "Maturity.Date", "MSA", "Delq.Status", 
    "MOD_FLAG", "Zero.Bal.Code", "ZB_DTE", "LPI_DTE", "FCC_DTE","DISP_DT", "FCC_COST", 
    "PP_COST", "AR_COST", "IE_COST", "TAX_COST", "NS_PROCS", "CE_PROCS", "RMW_PROCS", 
    "O_PROCS", "NON_INT_UPB", "PRIN_FORG_UPB_FHFA", "REPCH_FLAG", "PRIN_FORG_UPB_OTH", 
    "TRANSFER_FLG") %>% tolower

dir(raw_perf_data_path, full.names = T)
ct = list()
mapply(function(v,c) {
  ct[v] <<- substr(c,1,1)
}, Performance_Variables, Performance_ColClasses)


memory.size()
memory.limit()
mem_used()
gc()

fszdf = data.table(fp = dir("D:/data/fannie_mae/Performance_All/", full.names = T))
fszdf[,size_gb := file.size(fp)/1024/1024/1024]
nchunks = 500
fszdf[,tot_size_gb := sum(size_gb)]

setkey(fszdf,size_gb)
fszdf[,mean_size_gb := mean(size_gb)];fszdf

if(F) {
  shardby = "LOAN_ID"
  a = fread("d:/data/fannie_mae/Performance_All/Performance_2000Q1.txt", colClasses = Performance_ColClasses, col.names = Performance_Variables)
  a.df = shard(a,"loan_id", 300, outdir = "test1", overwrite = T)
  
  system.time(plot(density(a.df[,.N])))
  
  append = T
  
  csv_to_disk.frame("d:/data/fannie_mae/Performance_All/Performance_2000Q1.txt", "test", colClasses = Performance_ColClasses, col.names = Performance_Variables)
  
  a = csv_to_disk.frame("d:/data/fannie_mae/Performance_All/Performance_2000Q1.txt", "test", shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  b = csv_to_disk.frame("d:/data/fannie_mae/Performance_All/Performance_2000Q2.txt", "test1", shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  system.time(d <- csv_to_disk.frame("d:/data/fannie_mae/Performance_All/Performance_2000Q3.txt", "test3", shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables))
  
  a = disk.frame("test")
  b = disk.frame("test1")
  
  system.time(rbindlist.disk.frame(list(a,b,d),"test2"))
  
  dfiles = dir("d:/data/fannie_mae/Performance_All/",full.names = T)
  
    #dfiles = dir("D:/data/fannie_mae/a",full.names = T)
  
  # system.time(future_lapply(1:length(dfiles), function(i) {
  #   csv_to_disk.frame(dfiles[i], glue("test_fm/{i}"), shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  # }))
  system.time(future_lapply(1:length(dfiles), function(i) {
    csv_to_disk.frame(dfiles[i], glue("test_fm/dfiles[i]"), in_chunk_size = 1e7, shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  }))
  # 
  # ok = setdiff(1:length(dfiles), as.numeric(dir("D:/git/disk.frame/test_fm")))
  # 
  # system.time(future_lapply(ok, function(i) {
  #   csv_to_disk.frame(dfiles[i], glue("test_fm/{dfiles[i]}"), in_chunk_size = 1e7, shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  # }))
  # 
  # system.time(adf <- csv_to_disk.frame(
  #   "d:/data/fannie_mae/Performance_All/Performance_2000Q1.txt",
  #   outdir = "test",nchunks = 500, in_chunk_size = 1e6, shardby = "loan_id", colClasses = Performance_ColClasses,
  #   col.names = Performance_Variables,delim = "|",))

  
  # combine all the files into one big file
  rbindlist.disk.frame(map(dir("D:/git/disk.frame/test_fm",full.names = T),~disk.frame(.x)), outdir = "test_fm_comb")
  
  system.time(b = csv_to_disk.frame("d:/data/fannie_mae/Performance_All/Performance_2000Q2.txt", "test1", shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables))
  
  map(dir("test_fm/",full.names = T),~{
    print(.x);
    disk.frame(.x)[,.N]
    })
}



