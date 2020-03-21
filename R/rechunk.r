#' Increase or decrease the number of chunks in the disk.frame
#' @param df the disk.frame to rechunk
#' @param nchunks number of chunks
#' @param shardby the shardkeys
#' @param outdir the output directory
#' @param overwrite overwrite the output directory
#' @param shardby_function splitting of chunks: "hash" for hash function or "sort" for semi-sorted chunks
#' @param sort_splits for the "sort" shardby function, a dataframe with the split values.
#' @param desc_vars for the "sort" shardby function, the variables to sort descending.
#' @export
#' @examples
#' # create a disk.frame with 2 chunks in tempdir()
#' cars.df = as.disk.frame(cars, nchunks = 2)
#'
#' # re-chunking cars.df to 3 chunks, done "in-place" to the same folder as cars.df
#' rechunk(cars.df, 3)
#'
#' new_path = tempfile(fileext = ".df")
#' # re-chunking cars.df to 4 chunks, shard by speed, and done "out-of-place" to a new directory
#' cars2.df = rechunk(cars.df, 4, outdir=new_path, shardby = "speed")
#'
#' # clean up cars.df
#' delete(cars.df)
#' delete(cars2.df)
rechunk <- function(df, nchunks, outdir = attr(df, "path", exact=TRUE), shardby = NULL, overwrite = TRUE, shardby_function="hash", sort_splits=NULL, desc_vars=NULL) {
  
  # we need to force the chunks to be computed first as it's common to make nchunks a multiple of chunks(df)
  # but if we do it too late then the folder could be empty
  force(nchunks) 
  
  if (nchunks < 1) {
    stop(glue::glue("nchunks must be larger than 1"))
  }
  
  stopifnot("disk.frame" %in% class(df))
  
  user_had_not_set_shard_by = is.null(shardby)
  user_had_set_shard_by = !user_had_not_set_shard_by

  # back up the files if writing to the same directory
  if(outdir == attr(df,"path", exact=TRUE)) {
    back_up_tmp_dir <- tempfile("back_up_tmp_dir")
    fs::dir_create(back_up_tmp_dir)
    
    fs::dir_copy(
      file.path(outdir, ".metadata"), #from
      file.path(back_up_tmp_dir, ".metadata") #to
    )
    
    # back-up the files first
    full_files = dir(outdir, full.names = TRUE)
    short_files = dir(outdir)
    
    # move all files to the back up folder
    purrr::map(full_files, ~{
      fs::file_move(.x, back_up_tmp_dir)
    })
    
    if(fs::dir_exists(file.path(outdir, ".metadata"))) {
      fs::dir_delete(file.path(outdir, ".metadata"))
    }
    
    # TODO check for validity
    message(glue::glue("files have been backed up to temporary dir {back_up_tmp_dir}. You can recover there files until you restart your R session"))
    
    df = disk.frame(back_up_tmp_dir)
  }
  
  overwrite_check(outdir, overwrite)
  
  dfp = attr(df, "path", exact=TRUE)
  existing_shardkey = shardkey(df)
  
  # by default, if shardkey is defined then rechunk will continue to reuse it
  if (is.null(shardby)) {
    shardby = existing_shardkey[[1]]
  }


  if(user_had_set_shard_by) {
    return(hard_group_by(df, shardby, nchunks = nchunks, outdir = outdir, overwrite = TRUE, shardby_function=shardby_function, sort_splits=sort_splits, desc_vars=desc_vars))
  } else if (identical(shardby, "") | is.null(shardby)) {
    # if no existing shardby 
    nr = nrow(df)
    nr_per_chunk = ceiling(nr/nchunks)
    used_so_far = 0
    done = FALSE
    chunks_read = 1
    chunks_written = 0
    res = disk.frame(outdir)
    a = get_chunk(df, 1)
    used_so_far = nrow(a)
    while(chunks_read < nchunks(df)) {
      if((nr_per_chunk <= used_so_far)) {
        disk.frame::add_chunk(res, a[1:nr_per_chunk,])
        chunks_written = chunks_written + 1
        a = a[-(1:nr_per_chunk),]
        used_so_far = nrow(a)
      } else {
        chunks_read = chunks_read + 1
        newa = get_chunk(df, chunks_read)
        used_so_far = used_so_far + nrow(newa)
        a = rbindlist(list(a, newa))
        rm(newa)
      }
    }
    
    while(chunks_written < nchunks) {
      rows_to_write = min(nr_per_chunk, nrow(a))
      disk.frame::add_chunk(res, a[1:rows_to_write,])
      a = a[-(1:rows_to_write),]
      chunks_written = chunks_written + 1
    }
    
    return(res)
  } else if(!is.null(shardby)) { # if there is existing shard by; shardby has been replaced with new shard by
    if(user_had_not_set_shard_by) {
      warning("shardby = NULL; but there are already shardkey's defined for this disk.frame. Therefore a rechunk algorithm that preserves the shardkey's has been applied and this algorithm is slower than an algorithm that doesn't use a shardkey.")
    }
    
    # using some maths we can cut down on the number of operations
    nc = nchunks(df)
    
    # TODO there is bug here! If the chunks are in numbers form!
    # if the number of possible new chunk ids is one then no need to perform anything. just merge those
    possibles_new_chunk_id = purrr::map(1:nc, ~unique((.x-1 + (0:(nchunks-1))*nc) %% nchunks)+1)
    lp = purrr::map_int(possibles_new_chunk_id,length)
    
    #need to shards
    nts = which(lp != 1)
    
    bad_boys = future.apply::future_lapply(nts, function(chunk_id) {
      df1 = disk.frame::get_chunk(df, chunk_id)
      disk.frame::shard(df1, shardby, nchunks = nchunks, overwrite = TRUE)
    })
    
    # for those that don't need to be resharded
    tmp_fdlr = tempfile("rechunk_shard")
    fs::dir_create(tmp_fdlr)

    oks = furrr::future_map(which(lp == 1), function(i) {
      file_chunk = file.path(attr(df, "path", exact=TRUE), i %>% paste0(".fst"))
      fs::file_move(file_chunk, file.path(tmp_fdlr, possibles_new_chunk_id[[i]] %>% paste0(".fst")))
      disk.frame(tmp_fdlr)
    })
    
    list_of_sharded = c(bad_boys, oks)
    new_one <- rbindlist.disk.frame(list_of_sharded, outdir=outdir, by_chunk_id = TRUE, overwrite = overwrite)
    
    res = add_meta(new_one, nchunks = nchunks(new_one), shardkey = shardby, shardchunks = nchunks)
    return(res)
  } else {
    stop("rechunk: option not supported")
  }
}
