#' Increase or decrease the number of chunks in the disk.frame
#' @param df the disk.frame to rechunk
#' @param nchunks number of chunks
#' @param shardby the shardkeys
#' @param outdir the output directory
#' @param overwrite overwrite the output directory
#' @export
rechunk <- function(df, nchunks, outdir = attr(df, "path"), shardby = NULL, overwrite = T) {
  #browser()
  stopifnot("disk.frame" %in% class(df))
  user_had_not_set_shard_by = is.null(shardby)
  if(!is_disk.frame(outdir)) {
    stop(glue::glue("The folder {outdir} is not a disk.frame. Further actions stopped to prevent accidental deletion of important files"))
  }

  # back up the files if writing to the same directory
  if(outdir == attr(df,"path")) {
    back_up_tmp_dir <- tempfile("back_up_tmp_dir")
    fs::dir_create(back_up_tmp_dir)
    print(glue::glue("files have been backed up to temporary dir {back_up_tmp_dir}. You can recover there files until you restart your R session"))
    
    fs::dir_copy(
      file.path(outdir, ".metadata"), #from
      file.path(back_up_tmp_dir,".metadata") #to
      )
    
    if(fs::dir_exists(file.path(outdir, ".metadata"))) {
      fs::dir_delete(file.path(outdir, ".metadata"))
    }
    
    # back-up the files first
    full_files = dir(outdir, full.names = T)
    short_files = dir(outdir)
    
    # move all files to the back up folder
    purrr::map2(full_files, short_files, ~{
      fs::file_move(.x, file.path(back_up_tmp_dir, .y))
    })
    
    df = disk.frame(back_up_tmp_dir)
  }
  
  overwrite_check(outdir, overwrite)
  
  dfp = attr(df, "path")
  existing_shardkey = shardkey(df)
  
  # by default, if shardkey is defined then rechunk will continue to reuse it
  if (is.null(shardby)) {
    shardby = existing_shardkey[[1]]
  }

  if (is.null(shardby)) {
    nr = nrow(df)
    nr_per_chunk = ceiling(nr/nchunks)
    used_so_far = 0
    done = F
    i = 1
    a = get_chunk(df,1)
    used_so_far = used_so_far + nrow(a)
    while(!done) {
      if(nr_per_chunk <= used_so_far) {
        fst::write_fst(a, file.path(dfp, paste0(i,".fst")))
      } else {
        i = i + 1
        newa = get_chunk(df, i)
        used_so_far = used_so_far + nrow(newa)
        a = rbindlist(list(a, newa))
        rm(newa)
      }
    }
    res <- disk.frame(df)
    return(add_meta(res))
  } else {
    if(user_had_not_set_shard_by) {
      warning("shardby = NULL; but there are already shardkey's defined for this disk.frame. Therefore a rechunk algorithm that preserves the shardkey's has been applied and this algorithm is slower than an algorithm that doesn't use a shardkey.")
    }
    
    # using some maths we can cut down on the number of operations
    nc = nchunks(df)
    # if the number of possible new chunk ids is one then on need to perform anything. just merge those
    possibles_new_chunk_id = purrr::map(1:nc, ~unique((.x-1 + (0:(nchunks-1))*nc) %% nchunks)+1)
    lp = purrr::map_int(possibles_new_chunk_id,length)
    
    #need to shards
    nts = which(lp != 1)
    
    bad_boys = future.apply::future_lapply(nts, function(chunk_id) {
      df1 = disk.frame::get_chunk(df, chunk_id)
      disk.frame::shard(df1, shardby, nchunks = nchunks, overwrite = T)
    })
    
    # for those that don't need to be resharded
    oks = furrr::future_map(which(lp == 1), function(i) {
      file_chunk = file.path(attr(df, "path"), i %>% paste0(".fst"))
      tmp_fdlr = tempfile("rechunk_shard")
      fs::dir_create(tmp_fdlr)
      fs::file_move(file_chunk, file.path(tmp_fdlr, possibles_new_chunk_id[[i]] %>% paste0(".fst")))
      disk.frame(tmp_fdlr)
    })
    
    list_of_sharded = c(bad_boys, oks)
    
    system.time(new_one <- rbindlist.disk.frame(list_of_sharded, outdir=outdir, by_chunk_id = T, overwrite = overwrite))
    #browser()
    res = add_meta(new_one, nchunks = nchunks(new_one), shardkey = shardby, shardchunks = nchunks)
    return(res)
  }
}
