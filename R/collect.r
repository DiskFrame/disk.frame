#' Bring the disk.frame into R
#' @param df A disk.frame
#' @import purrr
#' @export 
collect.disk.frame <- function(df, ...) {
  #browser()
  map_dfr(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
  
  #rbindlist(lapply(1:nchunk(df), function(i) get_chunk.disk.frame(df, i)), ...)
}

#' Bring the disk.frame into R
#' @param df A disk.frame
#' @export
collect <- function(...) {
  UseMethod("collect")
}
