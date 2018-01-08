# library(fst)
# library(data.table)
# library(future)
# library(glue)
library(disk.frame)
# plan(multiprocess)
zipfile = "D:/data/fannie_mae/Performance_All.zip"
outpath = "fm.df"

validate_zip_to_disk.frame(zipfile, outpath)
