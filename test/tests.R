df <-disk.frame("d:/data/aaa.fst")
system.time(res <- chunks_lapply(df, nrow))

system.time(res <- chunks_sapply(df, nrow))

system.time(res <- chunks_sapply(df, function(chunk) {
  chunk[,max(ordinal), asdiflightplanid]
}))

system.time(res <- chunks_lapply(df, function(chunk) {
  chunk[,max(ordinal), asdiflightplanid]
},outdir = getwd()))


library(future)
plan(multiprocess)

system.time(res <- chunks_lapply(df, function(chunk) {
  chunk[,max(ordinal), asdiflightplanid]
}, parallel = T))


system.time(res <- chunks_lapply(df, function(chunk) {
  chunk[,max(ordinal), asdiflightplanid]
}, parallel = T))
system.time(rbindlist(res)[,max(V1), asdiflightplanid])


system.time(res <- chunks_lapply(df, function(chunk) {
  chunk[ordinal >= 100,]
}, outdir=getwd()))

res <- rbindlist(res)

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