library(fst)
library(data.table)
library(future)
library(future.apply)
library(glue)
library(disk.frame)
plan(multiprocess(workers= parallel::detectCores()/2)) # use only half the cores
#plan(sequential)
#source("R/disk.frame.r")
#source("R/zip_to_disk.frame.r")

zipfile = "D:/data/fannie_mae/Performance_All.zip"
outpath = "fm.df"

Performance_ColClasses = 
  c("character", "character", "character", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", "character", "character", "character", "character", "character", "character", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", "character")
Performance_Variables = c("LOAN_ID", "Monthly.Rpt.Prd", "Servicer.Name", "LAST_RT", "LAST_UPB", "Loan.Age", "Months.To.Legal.Mat" , "Adj.Month.To.Mat", "Maturity.Date", "MSA", "Delq.Status", "MOD_FLAG", "Zero.Bal.Code", "ZB_DTE", "LPI_DTE", "FCC_DTE","DISP_DT", "FCC_COST", "PP_COST", "AR_COST", "IE_COST", "TAX_COST", "NS_PROCS", "CE_PROCS", "RMW_PROCS", "O_PROCS", "NON_INT_UPB", "PRIN_FORG_UPB_FHFA", "REPCH_FLAG", "PRIN_FORG_UPB_OTH", "TRANSFER_FLG")


system.time(a <- fread("d:/data/fannie_mae/Performance_2000Q1.txt", colClasses = Performance_ColClasses, col.names =  ))

pt = proc.time()
# TODO: estimate the amount of time it takes
zip_to_disk.frame(zipfile, outpath, colClasses = Performance_ColClasses, col.names = Performance_Variables) # took about 50mins to convert
timetaken(pt)

 
# pt = proc.time()
# validate_zip_to_disk.frame(zipfile, outpath)
# timetaken(pt)

