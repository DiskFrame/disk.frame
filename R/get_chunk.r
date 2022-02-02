#' Obtain one chunk by chunk id
#' @param df a disk.frame
#' @param n the chunk id. If numeric then matches by number, if character then returns the chunk with the same name as n
#' @param keep the columns to keep
#' @param full.names whether n is the full path to the chunks or just a relative path file name. Ignored if n is numeric
#' @param ... passed to fst::read_fst or whichever read function is used in the backend
#' @export
#' @examples
#' cars.df = as.disk.frame(cars, nchunks = 2)
#' get_chunk(cars.df, 1)
#' get_chunk(cars.df, 2)
#' get_chunk(cars.df, 1, keep = "speed")
#' 
#' # if full.names = TRUE then the full path to the chunk need to be provided
#' get_chunk(cars.df, file.path(attr(cars.df, "path"), "1.fst"), full.names = TRUE)
#' 
#' # clean up cars.df
#' delete(cars.df)
get_chunk <- function(...) {
  UseMethod("get_chunk")
}


#' @rdname get_chunk
#' @importFrom fst read_fst
#' @export
get_chunk.disk.frame <- function(df, n, keep = NULL, full.names = FALSE, ..., partitioned_info=NULL) {
  stopifnot("disk.frame" %in% class(df))
  # keep_chunks = attr(df, "keep_chunks", exact=TRUE)
  
  path = attr(df,"path", exact=TRUE)
  
  # all the variables to keep in the attr from a previous srckeep
  keep1 = attr(df, "keep", exact=TRUE)
  
  recordings = attr(df, "recordings", exact=TRUE)
  filename = ""
  
  if (typeof(keep) == "closure") {
    # sometimes purrr::keep is picked up
    keep = keep1
  } else if(!is.null(keep1) & !is.null(keep)) {
    if (length(setdiff(keep, keep1)) > 0) {
      keep1_vars = paste0(keep1, collapse = ", ")
      keep_no_good_vars = setdiff(keep, keep1) %>% paste0(collapse = ", ")
      stop(
        sprintf(
          "This disk.frame has a `srckeep` containing these columns: `%s`. 
          You are trying to keep `%s`, which are not available.",
          paste0(keep1_vars, collapse=", "), paste0(keep_no_good_vars, collapse=", ")))
    }
    keep = intersect(keep1, keep)
    if (!all(keep %in% keep1)) {
      warning("some of the variables specified in keep = {keep} is not available")
    }
  } else if(is.null(keep)) {
    keep = keep1
  }
  
  if(is.numeric(n)) {
    filename = file.path(path, paste0(n,".fst"))
    if(!file.exists(filename)) {
      filename = list.files(path, full.names = TRUE)[n]  
    }
  } else {
    if (full.names) {
      filename = n
    } else {
      filename = file.path(path, n)
    }
  }
  
  
  # if the file you are looking for doesn't exist
  if (!fs::file_exists(filename)) {
    warning(glue("The chunk {filename} does not exist; returning an empty data.table"))
    notbl <- data.table()
    attr(notbl, "does not exist") <- TRUE
    return(notbl)
  }

  if (is.null(recordings)) {
    if(typeof(keep)=="closure") {
      tmp = fst::read_fst(filename, as.data.table = TRUE,...)
    } else {
      tmp = fst::read_fst(filename, columns = keep, as.data.table = TRUE,...)
    }
    
    if(!is.null(partitioned_info)) {
      res = tmp %>% 
        mutate(.check=1) %>% 
        full_join(partitioned_info %>% mutate(.check=1), by=".check") %>% 
        select(-.check, -fullpath, -.disk.frame.sub.path)
      return(res)
    } else{
      return(tmp)
    }
  } else {
    if(typeof(keep)=="closure") {
      tmp_df_input = fst::read_fst(filename, as.data.table = TRUE,...)
    } else {
      tmp_df_input = fst::read_fst(filename, columns = keep, as.data.table = TRUE,...)
    }
    
    if(!is.null(partitioned_info)) {
      res = tmp_df_input %>% 
        mutate(.check=1) %>% 
        full_join(partitioned_info %>% mutate(.check=1), by=".check") %>% 
        select(-.check, -fullpath, -.disk.frame.sub.path)
      return(play(res, recordings))
    } else{
      return(play(tmp_df_input, recordings))
    }
  }
}
