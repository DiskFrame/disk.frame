fst::write.fst(data.frame(a = sample(1:100, 500000000, replace = T)), "test.fst")

pt <- proc.time()
bbb = read.fst("test.fst", as.data.table = T)
bbb[,sum(as.numeric(a))]
timetaken(pt)
rm(bbb); gc()

library(future)
plan(multiprocess)
abc <- function(from, to) {
  bbb = read.fst("test.fst", from = from, to = to, as.data.table = T)
  bbb[,sum(as.numeric(a))]
}


pt <- proc.time()
a1 %<-% abc(1, 250000000)
a2 %<-% abc(250000001, 500000000)
print(a1 + a2)
timetaken(pt)
rm(a1)
rm(a2); gc()
