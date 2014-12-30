library(parallel)
cl <- makeCluster(getOption("cl.cores", 2))


parSapply(cl, 1:)

system.time(a <- data.table::fread("c:/testing/SOVN_FINALFILE_1406INCLHSab"))

system.time(a <- data.table::fread("c:/testing/AO_ACCOUNTLEVEL_1406_FIXaa",stringsAsFactors = T))
system.time(save(a, "c:/teting/SOVN_FINALFILE_1406INCLHS.df"))

system.time(read.csv("c:/testing/AO_ACCOUNTLEVEL_1406_FIXaa"))

library(data.table)
system.time(a.dt <- data.table(a))


a <- data.table::fread("c:/testing/AO_ACCOUNTLEVEL_1406_FIXab",stringsAsFactors = T)

setwd("c:/testing")
system.time(save(a,file="a.rdata"))


system.time(load(file="a.rdata"))
library(dplyr)
a1 <- group_by(a, V1)

summarise(a1, mean(V4))

expected <- paste0("AO_ACCOUNTLEVEL_1406_FIX",apply(expand.grid(letters,letters),1, paste0, collapse=""))
setwd("c:/testing")
actual <- dir()

all.files <- intersect(expected, actual)


make.rdata <- function(file.name) {
  setwd("C:/testing")
  a <- data.table::fread(file.name,stringsAsFactors = T)
  save(a, file = paste(file.name,".rdata"))
}

library(parallel)
mc <- makeCluster(2)
system.time(parSapply(mc, all.files, make.rdata))
stopCluster(mc)


