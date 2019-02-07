#' Apply the same function to all chunks
#' @param df a disk.frame
#' @param fn a function to apply to each of the chunks
#' @param outdir the output directory
#' @param keep the columns to keep from the input
#' @param chunks The number of chunks to output
#' @param lazy if TRUE then do this lazily
#' @param compress 0-100 fst compression ratio
#' @param overwrite if TRUE removes any existing chunks in the data
#' @import fst purrr
#' @importFrom future.apply future_lapply
#' @export
map.disk.frame <- function(df, fn, outdir = NULL, keep = NULL, chunks = nchunks(df), compress = 50, lazy = T, overwrite = F) {  
  ##browser
  fn = purrr::as_mapper(fn)
  if(lazy) {
    attr(df, "lazyfn") = c(attr(df, "lazyfn"), fn)
    return(df)
  }
  
  if(!is.null(outdir)) {
    overwrite_check(outdir, overwrite)
  }
  
  stopifnot(is_ready(df))
  
  keep1 = attr(df,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(df, "path")
  files <- list.files(path, full.names = T)
  files_shortname <- list.files(path)
  
  keep_future = keep
  res = future.apply::future_lapply(1:length(files), function(ii) {
    ds = disk.frame::get_chunk(df, ii, keep=keep_future)
    res = fn(ds)
    if(!is.null(outdir)) {
      fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  })
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}

#' imap.disk.frame accepts a two argument function where the first argument is a disk.frame and the 
#' second is the chunk ID
#' @export
#' @rdname map.disk.frame
imap.disk.frame <- function(df, fn, outdir = NULL, keep = NULL, chunks = nchunks(df), compress = 50, lazy = T, overwrite = F) {
  ##browser
  fn = purrr::as_mapper(fn)
  
  if(lazy) {
    attr(df, "lazyfn") = c(attr(df, "lazyfn"), fn)
    return(df)
  }
  
  if(!is.null(outdir)) {
    overwrite_check(outdir, overwrite)
  }
  
  stopifnot(is_ready(df))
  
  keep1 = attr(df,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(df, "path")
  files <- list.files(path, full.names = T)
  files_shortname <- list.files(path)
  
  keep_future = keep
  res = future.apply::future_lapply(1:length(files), function(ii) {
    ds = disk.frame::get_chunk(df, ii, keep=keep_future)
    res = fn(ds, ii)
    if(!is.null(outdir)) {
      fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  })
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}
  
#' @param ... passed to disk.frame::map.disk.frame
#' @export
#' @rdname map.disk.frame
chunk_lapply <- function (...) {
  warning("chunk_lapply is deprecated in favour of map.disk.frame")
  map.disk.frame(...)
}