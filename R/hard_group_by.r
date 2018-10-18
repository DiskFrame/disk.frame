
#' Perform a group by and ensuring that every unique grouping of by is
#' in the same chunk
#' @export
hard_group_by <- function(...) {
  UseMethod("hard_group_by")
}

#' Show a progress bar of the action being performed
#' @export
progressbar <- function(df) {
  if(attr(df,"performing") == "hard_group_by") {
    # create progress bar
    
    shardby = "acct_id"
    #browser()
    fparent = attr(df,"parent")
    
    #tmp = file.path(fparent,".performing","inchunks")
    tmp = "tmphardgroupby2"
    
    l = length(dir(fparent))
    pt_begin_split = proc.time()
    doprog <- function(pt_from, sleep = 1) {
      tkpb = winProgressBar(title = sprintf("Hard Group By Stage 1(/2) - %s", shardby), label = "Checking completeness",
                            min = 0, max = l*1.5, initial = 0, width = 500)
      pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
      
      on.exit(close(pb))
      on.exit(close(tkpb))
      while(length(dir(file.path(tmp,l))) < l) {
        wl = length(dir(file.path(tmp,1:l)))/l
        tt <- proc.time()[3] - pt_from[3]
        #browser()
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl) + avg_speed*l/2
        elapsed = round(tt/60,1)
        
        setWinProgressBar(tkpb, wl, 
                          title = sprintf("Hard Group By Stage 1(/2) - %s", shardby),
                          label = sprintf("%.0f out of %d; avg speed %.2f mins; elapsed %.1f mins; another %.1f mins", wl,l, round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(dir(file.path(tmp,l))), 
                          title = sprint("Group By - %s", shardby))
        Sys.sleep(sleep)
      }
    }
    doprog(pt_begin_split, 1)
    
    pt_begin_collate = proc.time()
    doprog2 <- function(pt_from, sleep = 1) {
      tkpb = winProgressBar(title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating", shardby), label = "Checking completeness",
                            min = 0, max = l*1.5, initial = 0, width = 600)
      pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
      
      on.exit(close(pb))
      on.exit(close(tkpb))
      while(length(dir("large_sorted")) < l) {
        wl = length(dir("large_sorted"))
        tt <- proc.time()[3] - pt_from[3]
        #browser()
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl)
        elapsed = round(tt/60,1)
        
        setWinProgressBar(tkpb, l + wl/2, 
                          title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating -- %.0f out of %d chunks processed;", shardby, wl, l),
                          label = sprintf("avg %.2f min/chunk; %.1f mins elapsed; %.1f mins remaining;", round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(dir("large_sorted")), 
                          title = sprint("Hard Group By - %s", shardby))
        Sys.sleep(sleep)
      }
    }
    doprog2(pt_begin_collate, 1)
  }
}

if(F) {
  library(disk.frame)
  library(fst)
  library(data.table)
  library(magrittr)
  
  df <- disk.frame("tmphardgroupby")
  by = "acct_id"
  outdir = "large_sorted"
  
  nrow(df)
  
  a = df[,acct_id]
  
  # plan(multiprocess, workers = 4)
  # hgb = hard_group_by(df, "acct_id", outdir, 4); 
  
  plan(multiprocess, workers = 7)
  hgb = hard_group_by(df, "acct_id", outdir, 7); 
  progressbar(hgb)
  2+2
}

#' Shard a data.frame/data.table into chunk and saves it into a disk.frame
shard <- function(df, shardby, nchunks, outdir, ..., append = F, overwrite = F) {
  setDT(df)
  if(length(shardby) == 1) {
    code = glue("df[,out.disk.frame.id := disk.frame:::hashstr2i({shardby}, nchunks)]")
  } else {
    shardby_list = glue("paste0({paste0(shardby,collapse=',')})")
    code = glue("df[,out.disk.frame.id := disk.frame:::hashstr2i({shardby_list}, nchunks)]")
  }
  

  eval(parse(text=code))
  
  if(dir.exists(outdir)) {
    if(!overwrite) {
      stop(glue("outdir '{outdir}' already exists and overwrite is FALSE"))
    }
  } else if(!dir.exists(outdir)) {
    dir.create(outdir)
  }
  
  df[,{
    write.fst(.SD, file.path(outdir, paste0(.BY, ".fst")), ...)
  }, out.disk.frame.id]
  
  disk.frame(outdir)
}

#' hard_group_by
hard_group_by.disk.frame <- function(df, by, outdir) {
  #browser()
  ff = dir(attr(df, "path"))
  
  # shard
  tmp_dirs  = chunk_lapply(df, function(df) {
    tmpdir = tempfile()
    dir.create(tmpdir)
    shard(df, shardby = by, nchunks = nchunk(df), outdir = tmpdir)
  }, lazy = F)
  
  # now rbindlist
  res = rbindlist.disk.frame(tmp_dirs)

  # clean up the tmp dir
  sapply(tmp_dirs, function(x) {
    unlink(attr(x, "path"))
  })
  
  res
}


#' The nb stands for non-blocking
hard_group_by_nb.disk.frame <- function(df, by, outdir, nworkers = NULL) {
  #browser()
  if(is.null(nworkers)) {
    nworkers = parallel::detectCores()
  }
  
  fpath = attr(df, "path")
  
  if(!dir.exists(outdir)) dir.create(outdir)
  
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
  #   fst_tmp <- fst::read.fst(aa, as.data.table = T, columns = "ACCOUNT_ID")
  #   nrow2 = base::nrow(fst_tmp)
  #   res = fst_tmp[,.N,ACCOUNT_ID][,out.disk.frame.id := hashstrtoi(ACCOUNT_ID, l)]
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
  if(dir.exists(tmp)) {
    unlink(tmp,T,T)
    dir.create(tmp)
  } else {
    dir.create(tmp)
  }
  
  sapply(file.path(tmp,1:l), dir.create)
  
  fperf = prepare_dir.disk.frame(df, ".performing", T)
  fperfinchunks = prepare_dir.disk.frame(df, ".performing/inchunks", T)
  fperfoutchunks = prepare_dir.disk.frame(df, ".performing/outchunks", T)
  
  # split the chunks into smaller chunks based on hash value
  tmp_throwaway = lapply(2:length(indexes), function(ii) {
    tmp_throwaway1 %<-% {
      inchunkindices = (indexes[ii-1]+1):indexes[ii]
      lapply(inchunkindices, function(i) {
        aa = a[i]
        pt = proc.time()
        print(i)
        fst_tmp <- fst::read.fst(aa, as.data.table = T)
        fst_tmp[,out.disk.frame.id := hashstr2i(acct_id, l)]
        
        fst_tmp[,{
          write.fst(.SD, file.path(tmp, .BY, paste0(i,".fst")), 100)
        }, out.disk.frame.id]
        
        ## write file to inidcate stage 1 is done
        ## stage 2 will check if all files are present and will start work on later
        
        file.create(file.path(fperfinchunks,i))
        rm(fst_tmp)
        gc()
        print(timetaken(pt))
        NULL
      })
      
      # wait for next phase
      while(length(dir(fperfinchunks)) < nchunk(df)) {
        Sys.sleep(0.5)
      }
      
      pt_begin_collate <- proc.time()
      fldrs = dir(tmp,full.names = T)
      l = length(fldrs)
      tmp_throwaway <- NULL
      lapply(inchunkindices, function(ii) {
        #browser()
        dtfn = fldrs[ii]
        
        tmptmp2 = rbindlist(lapply(dir(dtfn,full.names =  T), function(ddtfn) {
          fst::read.fst(ddtfn, as.data.table=T)
        }))
        
        setkey(tmptmp2, acct_id)
        fst::write.fst(tmptmp2, file.path("large_sorted",sprintf("%d.fst",ii)), 100);
        gc()
        
        file.create(file.path(fperfoutchunks,ii))
        gc()
        NULL
      })
      
      print(paste0("collate files took: ", timetaken(pt))); pt <- proc.time()
      timetaken(pt)
    }
    ii
  })
  res = disk.frame(outdir)
  attr(res,"performing") <- "hard_group_by"
  attr(res,"parent") <- attr(df, "path")
  res
}



