library(fst)
library(data.table)
library(future)
library(future.apply)
library(glue)
library(disk.frame)
library(magrittr)
library(purrr)
library(pryr)
library(tidyr)
#plan(multiprocess(workers= parallel::detectCores()/2)) # use only half the cores
plan(multiprocess(workers = 6))
#plan(sequential)
sapply(dir("R",full.names = T),source)
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

dfiles = dir(raw_perf_data_path, full.names = T)
short_dfiles = dir(raw_perf_data_path)

if(!dir.exists("test_fm")) {
  dir.create("test_fm")
}