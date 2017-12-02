library(data.table)
library(magrittr)
library(future)
library(fst)
library(stringtomod)

#nworkers = parallel::detectCores(logical=F)
nworkers = parallel::detectCores()
plan(multiprocess, workers = nworkers, gc = T)
options(future.globals.maxSize=Inf)

df <- disk.frame(inpath)
ramlim = 15*1024^3
# sort by algorithm
shardby = "acct_id"


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

# planning  
indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
tmp_throwaway <- NULL

dir.create("tmptmp")
file.create("tmptmp/dothis")
system.time(tmp_throwaway <- future_lapply(1:l, function(i) {
  if(!file.exists("tmptmp/dothis")) return(data.table(-1,-1,-1))
  aa = a[i]
  fst_tmp <- fst::read.fst(aa, as.data.table = T, columns = "acct_id")
  nrow2 = base::nrow(fst_tmp)
  res = fst_tmp[,.N,acct_id][,out.disk.frame.id := hashstrtoi(acct_id, l)]
  rm(fst_tmp);gc()
  ramsize = pryr::object_size(res)
  if(ramsize*l <= ramlim) {
    write.fst(res, sprintf("tmptmp/%d",i))
  } else {
    file.remove("tmptmp/dothis")
    return(data.table(-1,-1,-1))
  }
  
  nrow1 = base::nrow(res)
  
  rm(res)
  gc()
  data.table(ramsize, nrow2, nrow1)
}) %>% rbindlist)

pt_begin_split <- proc.time()
pt <- proc.time()
indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
tmp_throwaway <- NULL
for(ii in 2:length(indexes)) {
  tmp_throwaway %<-% lapply((indexes[ii-1]+1):indexes[ii], function(i) {
    aa = a[i]
    pt = proc.time()
    print(i)
    fst_tmp <- fst::read.fst(aa, as.data.table = T)
    fst_tmp[,out.disk.frame.id := hashstrtoi(acct_id, l)]
    
    fst_tmp[,{
      write.fst(.SD, file.path(tmp, .BY, paste0(i,".fst")), 100)
    }, out.disk.frame.id]
    rm(fst_tmp)
    gc()
    print(timetaken(pt))
    NULL
  })
}

#, future.lazy = T, future.globals = c("a","l")
print(timetaken(pt))
# 2.5hours

# create progress bar
doprog <- function(pt_from, sleep = 1) {
  tkpb = winProgressBar(title = sprintf("Group By - %s", shardby), label = "Checking completeness",
                        min = 0, max = l, initial = 0, width = 500)
  pb <- txtProgressBar(min = 0, max = l, style = 3)
  
  on.exit(close(pb))
  on.exit(close(tkpb))
  while(length(dir(file.path(tmp,l))) < l) {
    wl = length(dir(file.path(tmp,1:l)))/l
    tt <- proc.time()[3] - pt_from[3]
    #browser()
    avg_speed = tt/wl
    pred_speed = avg_speed*(l-wl)
    elapsed = round(tt/60,1)
    
    setWinProgressBar(tkpb, wl, 
                      title = sprintf("Group By - %s", shardby),
                      label = sprintf("%.0f out of %d; avg speed %.2f mins; elapsed %.1f mins; another %.1f mins", wl,l, round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
    setTxtProgressBar(pb, length(dir(file.path(tmp,l))), 
                      title = sprint("Group By - %s", shardby))
    Sys.sleep(1)
  }
}
doprog(pt_begin_split, 20)


print(paste0("sorting took: ", timetaken(pt))); pt <- proc.time()

print(paste("started collating into sorted", Sys.time()))
# collate the files in each folder and write out into sorted form
pt_begin_collate <- proc.time()
fldrs = dir(tmp,full.names = T)
l = length(fldrs)
indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
tmp_throwaway <- NULL
for(iii in 2:length(indexes)) {
  #for(iii in split(1:l, 1:nworkers)) {
  system.time(tmp_throwaway %<-% lapply((indexes[iii-1]+1):indexes[iii], function(ii) {
    dtfn = fldrs[ii]
    tmptmp <- {
      tmptmp2 = lapply(dir(dtfn,full.names =  T), function(ddtfn) {
        fst::read.fst(ddtfn, as.data.table=T)
      }) %>% rbindlist
      
      setkey(tmptmp2, acct_id)
      tmptmp2 %>% 
        fst::write.fst(file.path("large_sorted",sprintf("%d.fst",ii)), 100);
      gc()}
    gc()
    NULL
  }))
}

doprog2 <- function(pt_from, sleep = 1) {
  tkpb = winProgressBar(title = sprintf("Group By - %s -- Stage 2 (of 2) collating", shardby), label = "Checking completeness",
                        min = 0, max = l, initial = 0, width = 600)
  pb <- txtProgressBar(min = 0, max = l, style = 3)
  
  on.exit(close(pb))
  on.exit(close(tkpb))
  while(length(dir("large_sorted")) < l) {
    wl = length(dir("large_sorted"))
    tt <- proc.time()[3] - pt_from[3]
    #browser()
    avg_speed = tt/wl
    pred_speed = avg_speed*(l-wl)
    elapsed = round(tt/60,1)
    
    setWinProgressBar(tkpb, wl, 
                      title = sprintf("Group By - %s -- Stage 2 (of 2) collating -- %.0f out of %d chunks processed;", shardby, wl, l),
                      label = sprintf("avg %.2f min/chunk; %.1f mins elapsed; %.1f mins remaining;", round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
    setTxtProgressBar(pb, length(dir("large_sorted")), 
                      title = sprint("Group By - %s", shardby))
    Sys.sleep(sleep)
  }
}
doprog2(pt_begin_collate,10)
print(paste0("collate files took: ", timetaken(pt))); pt <- proc.time()

timetaken(pt)
#}

pt_begin_collate <- proc.time()
fldrs = dir(tmp,full.names = T)
l = length(fldrs)
indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
tmp_throwaway <- NULL
for(iii in 2:length(indexes)) {
  #for(iii in split(1:l, 1:nworkers)) {
  system.time(tmp_throwaway %<-% lapply((indexes[iii-1]+1):indexes[iii], function(ii) {
    if(file.exists(file.path("large_sorted",sprintf("%d.fst",ii)))) {
      
    }
    dtfn = fldrs[ii]
    tmptmp <- {
      tmptmp2 = lapply(dir(dtfn,full.names =  T), function(ddtfn) {
        fst::read.fst(ddtfn, as.data.table=T)
      }) %>% rbindlist
      
      setkey(tmptmp2, acct_id)
      tmptmp2 %>% 
        fst::write.fst(file.path("large_sorted",sprintf("%d.fst",ii)), 100);
      gc()}
    gc()
    NULL
  }))
}


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


