#' Show a progress bar of the action being performed
#' @importFrom utils txtProgressBar setTxtProgressBar
#' @param df a disk.frame
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
      #tkpb = winProgressBar(title = sprintf("Hard Group By Stage 1(/2) - %s", shardby), label = "Checking completeness",
      #                      min = 0, max = l*1.5, initial = 0, width = 500)
      pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
      
      on.exit(close(pb))
      # on.exit(close(tkpb))
      while(length(list.files(file.path(tmp,l))) < l) {
        wl = length(list.files(file.path(tmp,1:l)))/l
        tt <- proc.time()[3] - pt_from[3]
        #list.files(
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl) + avg_speed*l/2
        elapsed = round(tt/60,1)
        
        #setWinProgressBar(tkpb, wl, 
        #                  title = sprintf("Hard Group By Stage 1(/2) - %s", shardby),
        #                  label = sprintf("%.0f out of %d; avg speed %.2f mins; elapsed %.1f mins; another %.1f mins", wl,l, round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(list.files(file.path(tmp,l))), 
                          title = sprintf("Group By - %s", shardby))
        Sys.sleep(sleep)
      }
    }
    doprog(pt_begin_split, 1)
    
    pt_begin_collate = proc.time()
    doprog2 <- function(pt_from, sleep = 1) {
      # tkpb = winProgressBar(title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating", shardby), label = "Checking completeness",
                            # min = 0, max = l*1.5, initial = 0, width = 600)
      pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
      
      on.exit(close(pb))
      # on.exit(close(tkpb))
      while(length(list.files("large_sorted")) < l) {
        wl = length(list.files("large_sorted"))
        tt <- proc.time()[3] - pt_from[3]
        #list.files(
        avg_speed = tt/wl
        pred_speed = avg_speed*(l-wl)
        elapsed = round(tt/60,1)
        
        # setWinProgressBar(tkpb, l + wl/2, 
        #                   title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating -- %.0f out of %d chunks processed;", shardby, wl, l),
        #                   label = sprintf("avg %.2f min/chunk; %.1f mins elapsed; %.1f mins remaining;", round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
        setTxtProgressBar(pb, length(list.files("large_sorted")), 
                          title = sprintf("Hard Group By - %s", shardby))
        Sys.sleep(sleep)
      }
    }
    doprog2(pt_begin_collate, 1)
  }
}

#' Perform a group by and ensuring that every unique grouping of by is
#' in the same chunk
#' @param df a disk.frame
#' @param ... grouping variables
#' @param outdir the output directory
#' @param nchunks The number of chunks in the output. Defaults = nchunks.disk.frame(df)
#' @param overwrite overwrite the out put directory
#' @export
hard_group_by <- function(df, ..., add = FALSE, .drop = FALSE) {
  UseMethod("hard_group_by")
}

#' @rdname hard_group_by
#' @export
#' @importFrom dplyr group_by
hard_group_by.data.frame <- function(df, ..., add = FALSE, .drop = FALSE) {
  dplyr::group_by(df, ..., add = FALSE, .drop = FALSE)
}

#' @rdname hard_group_by
#' @importFrom purrr map
#' @importFrom purrr map
#' @export
hard_group_by.disk.frame <- function(df, ..., outdir=tempfile("tmp_disk_frame_hard_group_by"), nchunks = disk.frame::nchunks(df), overwrite = T) {
  #browser()
  overwrite_check(outdir, overwrite)
  
  ff = list.files(attr(df, "path"))
  
  # test if the unlist it will error
  
  tryCatch({
    # This will return the variable names
    
    # TODO use better ways to do NSE
    by <- unlist(list(...))
    
    # shard and create temporary diskframes
    tmp_df  = map(df, function(df1) {
      tmpdir = tempfile()
      shard(df1, shardby = by, nchunks = nchunks, outdir = tmpdir, overwrite = T)
    }, lazy = F)
    
    
    # now rbindlist
    res = rbindlist.disk.frame(tmp_df, outdir=outdir, overwrite = overwrite)
    
    # clean up the tmp dir
    purrr::walk(tmp_df, ~{
      fs::dir_delete(attr(.x, "path"))
    })
    
    #browser()
    res1 <- NULL
    if(length(by) == 1) {
      res1 = res %>% dplyr::group_by({{by}}) 
    } else {
      eval(parse(text = glue::glue('res1 = group_by(res, {paste(by,collapse=",")})')))
    }
    
    res1
  }, error = function(e) {
    print(e)
    # This will return the variable names
    by = rlang::enquos(...) %>% 
      substr(2, nchar(.))
    
    # shard and create temporary diskframes
    tmp_df  = map(df, function(df1) {
      ##browser
      tmpdir = tempfile()
      shard(df1, shardby = by, nchunks = nchunks, outdir = tmpdir, overwrite = T)
    }, lazy = F)
    
    # now rbindlist
    res = rbindlist.disk.frame(tmp_df, outdir=outdir, overwrite = overwrite)
    
    # clean up the tmp dir
    purrr::walk(tmp_df, ~{
      fs::dir_delete(attr(.x, "path"))
    })
    
    res1 = res %>% dplyr::group_by(!!!syms(by))
    
    res1
  })
}
