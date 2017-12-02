library(data.table)
library(magrittr)
library(future)
library(fst)
# library(stringtomod)

#nworkers = parallel::detectCores(logical=F)
nworkers = parallel::detectCores()
plan(multiprocess, workers = nworkers, gc = T)
options(future.globals.maxSize=Inf)

df <- disk.frame(inpath)
ramlim = 15*1024^3
# sort by algorithm
shardby = "acct_id"


dt = data.table(
  acct_id = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # small groups (char)
  v3 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric e.g. 23.5749
)



# algorithm ---------------------------------------------------------------
#shardby.disk.frame <- function(df, shardby) {
pt <- proc.time()
print(paste("started processing groupby", Sys.time()))
pt_begin <- proc.time()
a = dir(attr(df,"path"), full.names = T)

# parallel read the column to groupby
l = length(a)

# tmp = tempfile("disk.frame.tmp")
# dir.create(tmp)
invisible(sapply(file.path(tmp,1:length(a)), dir.create))

if(interactive()) {
  large_sorted <- disk.frame("large_sorted")
  system.time(future_lapply(1:520, function(ll) {
    a = read.fst(sprintf("large_sorted/%d.fst",ll), as.data.table = T)
    write.fst(a, paste0(ll,".fst"), 100)
    file.remove(paste0(ll,".fst"))
    ll
  })) # 1823 seconds so 30mins to read and write
  
  #system.time(one1 <- large_sorted[1,]) # 860 seconds (15 mins) to read through the data
  system.time(one1 <- large_sorted[1,]) # 570 now so under 10 minutes
  system.time(res <- large_sorted[,.N, MONTH_KEY, keep = "MONTH_KEY"]) # 35 seconds
  system.time(res1 <- res[,sum(N), MONTH_KEY])
  res1
  res1[,sum(V1)]
  
  system.time(ccs1 <- read.fst("large_sorted/1.fst", as.data.table = T)) # 2 seconds
  
  system.time(write.fst(ccs1,"delete_it.fst", 100)) #takes about 4-6
  system.time(write.fst(ccs1,"delete_it.fst", 100)) #takes about 4-6
  system.time(write.fst(ccs1,"delete_it.fst")) #takes about 5-11
  system.time(write.fst(ccs1,"delete_it.fst")) #takes about 5-11
  
  #plan(multiprocess, workers = 16)#
  #system.time(res <- large_sorted[,.N, MONTH_KEY, keep = "MONTH_KEY"]) # slower
}