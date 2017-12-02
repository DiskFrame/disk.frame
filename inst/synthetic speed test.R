library(data.table)
library(fst)
library(future)
plan(multiprocess)

system.time(res <- data.table(
  id = sample(1:2500000, 250000000, replace = T),
  val = runif(250000000))[,id_str := sprintf("id%d", id)]) # 202 seconds
  
system.time(write.fst(res, "test.fst")) #102 seconds
system.time(write.fst(res, "test100.fst", 100)) #

rm(res); gc()

df <- disk.frame("test.fst")
system.time(df[,sum(val), id][,sum(V1), id]) # 48 #53seconds #16 seconds
system.time(df[
  keep = c("val", "id"), # keep only these variables
  , # filtering -- equivalent of where statements
  sum(val), # action
  by = id        # group by
  ][,sum(V1), id]) # 48 #53seconds #16 seconds

df100 <- disk.frame("test100.fst")
system.time(df100[,sum(val), id][,sum(V1), id]) # 59 #16 seconds
system.time(df100[,sum(val), id, keep = c("val", "id")][,sum(V1), id]) # 48 #53seconds #16 seconds


system.time(distribute(df, "testf.fst")) # 177.98 seconds
system.time(distribute(df, "testf100.fst", compress = 100)) #216.76


dff <- disk.frame("testf.fst")
system.time(res <- dff[,sum(val), id][,sum(V1), id]) # 56 seconds; if cheats by not reading everything in 15 seconds!!

dff100 <- disk.frame("testf100.fst")
system.time(res <- dff100[,sum(val), id][,sum(V1), id]) # 56 seconds; if cheats by not reading everything in 15 seconds!!


# pt <- proc.time()
# bbb = read.fst("test.fst", as.data.table = T) 
# bbb[,sum(as.numeric(a))]
# timetaken(pt)
# rm(bbb); gc()
# 
# library(future)
# plan(multiprocess)
# abc <- function(from, to) {
#   bbb = read.fst("test.fst", from = from, to = to, as.data.table = T)
#   bbb[,sum(as.numeric(a))]
# }
# 
# 
# pt <- proc.time()
# a1 %<-% abc(1, 250000000)
# a2 %<-% abc(250000001, 500000000)
# print(a1 + a2)
# timetaken(pt)
# rm(a1)
# rm(a2); gc()
