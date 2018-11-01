#' Show a progress bar of the action being performed
progressbar <- function(df) {
  if(attr(df,"performing") == "hard_group_by") {
    # create progress bar
    
    shardby = "acct_id"
    #list.files(
    fparent = attr(df,"parent")
    
    #tmp = file.path(fparent,".performing","inchunks")
    tmp = "tmphardgroupby2"
    
    l = length(list.files(fparent))
    pt_begin_split = proc.time()
    doprog <- function(pt_from, sleep = 1) {
      tkpb = winProgressBar(title = sprintf("Hard Group By Stage 1(/2) - %s", shardby), label = "Checking completeness",
                            min = 0, max = l*1.5, initial = 0, width = 500)
      pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
      
      on.exit(close(pb))
      on.exit(close(tkpb))
      while(length(list.files(file.path(tmp,l))) < l) {
        wl = length(list.files(file.path(tmp,1:l)))/l
        tt <- proc.time()[3] - pt_from[3]
        #list.files(
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl) + avg_speed*l/2
        elapsed = round(tt/60,1)
        
        setWinProgressBar(tkpb, wl, 
                          title = sprintf("Hard Group By Stage 1(/2) - %s", shardby),
                          label = sprintf("%.0f out of %d; avg speed %.2f mins; elapsed %.1f mins; another %.1f mins", wl,l, round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(list.files(file.path(tmp,l))), 
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
      while(length(list.files("large_sorted")) < l) {
        wl = length(list.files("large_sorted"))
        tt <- proc.time()[3] - pt_from[3]
        #list.files(
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl)
        elapsed = round(tt/60,1)
        
        setWinProgressBar(tkpb, l + wl/2, 
                          title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating -- %.0f out of %d chunks processed;", shardby, wl, l),
                          label = sprintf("avg %.2f min/chunk; %.1f mins elapsed; %.1f mins remaining;", round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(list.files("large_sorted")), 
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


#' Output a data.frame into disk.frame
#' @import glue fst fs
output_disk.frame <- function(df, outdir, nchunks, overwrite, shardkey, shardchunks, ...) {
  if(dir.exists(outdir)) {
    if(!overwrite) {
      stop(glue("outdir '{outdir}' already exists and overwrite is FALSE"))
    }
  } else {
    fs::dir_create(outdir)
  }
  
  df[,{
    if (base::nrow(.SD) > 0) {
      write_fst(.SD, file.path(outdir, paste0(.BY, ".fst")))
      NULL
    }
    NULL
  }, .out.disk.frame.id]
  res = disk.frame(outdir)
  add_meta(res, shardkey = shardkey, shardchunks = shardchunks)
}

#' Make a data.frame into a disk.frame
#' @import data.table fst
#' @export
as.disk.frame <- function(df, outdir, nchunks = recommend_nchunks(df), overwrite = F, ...) {
  #browser()
  if(overwrite & fs::dir_exists(outdir)) {
    fs::dir_delete(outdir)
    fs::dir_create(outdir)
  } else {
    fs::dir_create(outdir)
  }
  
  setDT(df)
  
  odfi = rep(1:nchunks, each = ceiling(nrow(df)/nchunks))
  odfi = odfi[1:nrow(df)]
  df[, .out.disk.frame.id := odfi]
  
  output_disk.frame(df, outdir, nchunks, overwrite, shardkey="", shardchunks=-1, ...)
}

#' Shard a data.frame/data.table into chunk and saves it into a disk.frame
#' @param df A disk.frame
#' @param shardby The column(s) to shard the data by.
#' @param nchunks The number of chunks
#' @param outdir The output directory of the disk.frame
#' @param overwrite If TRUE then the chunks are overwritten
#' @import glue 
#' @import fst
#' @export
shard <- function(df, shardby, outdir = tempfile("tmp_disk_frame_shard"), ..., nchunks = recommend_nchunks(df), overwrite = F) {
  if(!is.null(outdir)) {
    if(overwrite & fs::dir_exists(outdir)) {
      fs::dir_delete(outdir)
      fs::dir_create(outdir)
    } else {
      fs::dir_create(outdir)
    }
  }
  
  setDT(df)
  if(length(shardby) == 1) {
    code = glue::glue("df[,.out.disk.frame.id := disk.frame:::hashstr2i(as.character({shardby}), nchunks)]")
  } else {
    shardby_list = glue::glue("paste0({paste0(shardby,collapse=',')})")
    code = glue::glue("df[,.out.disk.frame.id := disk.frame:::hashstr2i({shardby_list}, nchunks)]")
  }
  
  eval(parse(text=code))
  
  output_disk.frame(df, outdir, nchunks, overwrite, shardkey = shardby, shardchunks = nchunks)
}

#' Perform a group by and ensuring that every unique grouping of by is
#' in the same chunk
#' @param df a disk.frame
#' @param by the columns to shard by
#' @param outdir the output directory
#' @param nchunks The number of chunks in the output. Defaults = nchunks.disk.frame(df)
#' @export
hard_group_by <- function(...) {
  UseMethod("hard_group_by")
}

#' @rdname hard_group_by
#' @import purrr
#' @export
hard_group_by.disk.frame <- function(df, by, outdir=tempfile("tmp_disk_frame_hard_group_by"), nchunks = nchunk.disk.frame(df), overwrite = T) {
  if(!is.null(outdir)) {
    if(overwrite & fs::dir_exists(outdir)) {
      fs::dir_delete(outdir)
      fs::dir_create(outdir)
    } else {
      fs::dir_create(outdir)
    }
  }
  
  ff = list.files(attr(df, "path"))
  
  # shard and create temporary diskframes
  tmp_df  = map.disk.frame(df, function(df1) {
    tmpdir = tempfile()
    shard(df1, shardby = by, nchunks = nchunks, outdir = tmpdir, overwrite = T)
  }, lazy = F)
  
  # now rbindlist
  res = rbindlist.disk.frame(tmp_df, outdir=outdir, overwrite = overwrite)

  # clean up the tmp dir
  purrr::map(tmp_df, ~{
    fs::dir_delete(attr(.x, "path"))
    #unlink()
  })
  
  res
}


#' The nb stands for non-blocking
#' TODO make it work!
hard_group_by_nb.disk.frame <- function(df, by, outdir, nworkers = NULL) {
  #list.files(
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
  #     write_fst(res, sprintf("tmptmp/%d",i))
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
  a = list.files(fpath, full.names =T)
  
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
          fst::write_fst(.SD, file.path(tmp, .BY, paste0(i,".fst")), 100)
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
      while(length(list.files(fperfinchunks)) < nchunk(df)) {
        Sys.sleep(0.5)
      }
      
      pt_begin_collate <- proc.time()
      fldrs = list.files(tmp,full.names = T)
      l = length(fldrs)
      tmp_throwaway <- NULL
      lapply(inchunkindices, function(ii) {
        #list.files(
        dtfn = fldrs[ii]
        
        tmptmp2 = rbindlist(lapply(list.files(dtfn,full.names =  T), function(ddtfn) {
          fst::read.fst(ddtfn, as.data.table=T)
        }))
        
        setkey(tmptmp2, acct_id)
        fst::write_fst(tmptmp2, file.path("large_sorted",sprintf("%d.fst",ii)), 100);
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



