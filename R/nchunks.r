#' Return the number of chunks
#' @param df a disk.frame
#' @param skip.ready.chunk NOT implemented
#' @export
nchunks <- function(...) {
  UseMethod("nchunks")
}

#' Returns the number of chunks in a disk.frame
#' @export
#' @rdname nchunks
nchunk <- function(...) {
  UseMethod("nchunk")
}

#' @export
nchunk.disk.frame <- function(...) nchunks.disk.frame(...)

#' @export
#' @import fs
nchunks.disk.frame <- function(df, skip.ready.check = F) {
  #browser()
  #if(!skip.ready.check) stopifnot(is_ready(df))
  fpath <- attr(df,"path")
  if(is.dir.disk.frame(df)) {
    return(length(fs::dir_ls(fpath, type="file")))
  } else {
    return(1)
  }
}