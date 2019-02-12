library(glue)
library(purrr)
library(fst)
library(tidyr)
library(ggplot2)
library(stringr)
library(xgboost)
library(lubridate)
library(disk.frame)

#raw_perf_data_path = "C:/data/Performance_All/"
raw_perf_data_path = "d:/data/Performance_All_small/"

# where the outputs go
#outpath = "c:/data/fannie_mae_disk_frame/"
outpath = "d:/data/fannie_mae_disk_frame_small/"

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

Acquisitions_Variables = c("LOAN_ID", "ORIG_CHN", "Seller.Name", "ORIG_RT", "ORIG_AMT", "ORIG_TRM", "ORIG_DTE"
                           ,"FRST_DTE", "OLTV", "OCLTV", "NUM_BO", "DTI", "CSCORE_B", "FTHB_FLG", "PURPOSE", "PROP_TYP"
                           ,"NUM_UNIT", "OCC_STAT", "STATE", "ZIP_3", "MI_PCT", "Product.Type", "CSCORE_C", "MI_TYPE", "RELOCATION_FLG") %>% tolower()

Acquisition_ColClasses = c("character", "character", "character", "numeric", "numeric", "integer", "character", "character", "numeric",
                           "numeric", "character", "numeric", "numeric", "character", "character", "character", "character", "character",
                           "character", "character", "numeric", "character", "numeric", "numeric", "character")
