#' Bring the disk.frame into R as data.table/data.frame
#' @import purrr
#' @export
#' @rdname collect
collect.disk.frame <- function(df, ...) {
  #list.files(
  if(nchunks(df) > 0) {
    purrr::map_dfr(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
  } else {
    data.table()
  }
}

#' Bring the disk.frame into R as list
#' @import purrr
#' @export
#' @rdname collect
collect_list <- function(df, ... , simplify = F) {
  #list.files(
  if(nchunks(df) > 0) {
    res = purrr::map(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    if (simplify) {
      return(simplify2array(res))
    } else {
      return(res)
    }
  } else {
    list()
  }
}

#' Bring the disk.frame into R
#' @param df A disk.frame
# collect <- function(df, ...) {
#   UseMethod("collect")
# }
