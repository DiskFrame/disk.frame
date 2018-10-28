#' Bring the disk.frame into R
#' @import purrr
#' @export
#' @rdname collect
collect.disk.frame <- function(df, ...) {
  #browser()
  map_dfr(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
}

#' Bring the disk.frame into R
#' @param df A disk.frame
# collect <- function(df, ...) {
#   UseMethod("collect")
# }
