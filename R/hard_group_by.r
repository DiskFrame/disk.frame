
#' Perform a group by and ensuring that every unique grouping of by is
#' in the same chunk
#' @export
hard_group_by <- function(...) {
  UseMethod("hard_group_by")
}

hard_group_by_progress <- function(df) {
  
}

#' Show a progress bar of the action being performed
#' @export
progressbar <- function(df) {
  if(attr(df,"performing") == "hard_group_by") {
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
  }
}

if(F) {
  library(disk.frame)
  
  df <- disk.frame("tmphardgroupby")
  by = "acct_id"
  outdir = "tmphgb2"
}

#' @import hashstr2i
hard_group_by.disk.frame <- function(df, by, outdir) {
  nworkers = parallel::detectCores()
  fpath = attr(df, "path")
  attr(df, "performing") <- "hard_group_by"
  
  # indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
  # tmp_throwaway <- NULL
  
  # create a tmp folder
  # if(!dir.exists(file.path(fpath,".tmp"))) {
  #   dir.create(file.path(fpath,".tmp"))
  # }
  # 
  # file.create(file.path(fpath,".tmp"))
  # system.time(tmp_throwaway <- future_lapply(1:l, function(i) {
  #   if(!file.exists("tmptmp/dothis")) return(data.table(-1,-1,-1))
  #   aa = a[i]
  #   fst_tmp <- fst::read.fst(aa, as.data.table = T, columns = "acct_id")
  #   nrow2 = base::nrow(fst_tmp)
  #   res = fst_tmp[,.N,acct_id][,out.disk.frame.id := hashstrtoi(acct_id, l)]
  #   rm(fst_tmp);gc()
  #   ramsize = pryr::object_size(res)
  #   if(ramsize*l <= ramlim) {
  #     write.fst(res, sprintf("tmptmp/%d",i))
  #   } else {
  #     file.remove("tmptmp/dothis")
  #     return(data.table(-1,-1,-1))
  #   }
  #   
  #   nrow1 = base::nrow(res)
  #   
  #   rm(res)
  #   gc()
  #   data.table(ramsize, nrow2, nrow1)
  # }) %>% rbindlist)
  
  pt_begin_split <- proc.time()
  pt <- proc.time()
  l = nchunk(df)
  indexes = unique(round(seq(0, l, length.out = nworkers+1),0))
  fpath = attr(df,"path")
  a = dir(fpath, full.names =T)
  
  tmp = "tmphardgroupby2"
  dir.create(tmp)
  sapply(file.path(tmp,1:l), dir.create)
  
  tmp_throwaway = lapply(2:length(indexes), function(ii) {
    tmp_throwaway1 %<-% lapply((indexes[ii-1]+1):indexes[ii], function(i) {
      aa = a[i]
      pt = proc.time()
      print(i)
      fst_tmp <- fst::read.fst(aa, as.data.table = T)
      fst_tmp[,out.disk.frame.id := hashstr2i::hashstr2i(acct_id, l)]
      
      fst_tmp[,{
        write.fst(.SD, file.path(tmp, .BY, paste0(i,".fst")), 100)
      }, out.disk.frame.id]
      rm(fst_tmp)
      gc()
      print(timetaken(pt))
      NULL
      
      write.fst(file.path)
    })
    ii
  })
  
  #, future.lazy = T, future.globals = c("a","l")
  print(timetaken(pt))
  # 2.5hours
  
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
}



