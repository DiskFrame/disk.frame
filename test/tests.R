# a <- disk.frame("c:/testing/AO_ACCOUNTLEVEL_1406_FIX_bk/")
# 
# system.time(a.mut <- mutate(a, speed1 = paste0(AccountNumber,"0")))
# 
# 
# 
# 
# f = unz("c:/testing/AO_ACCOUNTLEVEL_1406_FIX.zip", "AO_ACCOUNTLEVEL_1406_FIX.csv",open ="r")
# file.con <- open(f)
# 
# 
# 
# read.csv(f)
# 
# f <- file("c:/testing/AO_ACCOUNTLEVEL_1406_FIX.csv")
#       
# file.size <- function(file) {
#   as.numeric(file.info(file)["size"])
# }
# 
# csv.to.disk.frame(infile = "c:/testing/AO_ACCOUNTLEVEL_1406_FIX.csv",outpath = "c:/testing/AO_ACCOUNTLEVEL_1406_FIX")
# 
# 
# 
# system.time(csv.to.disk.frame(infile = "c:/testing/SOVN_FINALFILE_1406INCLHS.csv", outpath = "c:/testing/SOVN_FINALFILE_1406INCLHS"))
# 
# system.time(csv.to.disk.frame(infile = "c:/testing/AO_SEGMENTATION_DATA_1312_FULL.csv", outpath = "c:/testing/AO_SEGMENTATION_DATA_1312_FULL"))