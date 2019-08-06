#' Return the number of chunks
#' @param df a disk.frame
#' @param skip.ready.check NOT implemented
#' @param ... not used
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # return the number of chunks
#' nchunks(cars.df)
#' nchunk(cars.df)
#' 
#' # clean up cars.df
#' delete(cars.df)
nchunks <- function(df, ...) {
  UseMethod("nchunks")
}

#' Returns the number of chunks in a disk.frame
#' @rdname nchunks
#' @export
nchunk <- function(df, ...) {
  UseMethod("nchunk")
}

#' @rdname nchunks
#' @export
nchunk.disk.frame <- function(df, ...) {
  nchunks.disk.frame(df, ...)
}

#' @importFrom fs dir_ls
#' @rdname nchunks
#' @export
nchunks.disk.frame <- function(df, skip.ready.check = FALSE, ...) {
  #if(!skip.ready.check) stopifnot(is_ready(df))
  fpath <- attr(df,"path")
  if(is.dir.disk.frame(df)) {
    return(length(fs::dir_ls(fpath, type="file")))
  } else {
    return(1)
  }
}