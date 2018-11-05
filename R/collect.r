#' Bring the disk.frame into R as data.table/data.frame
#' @import purrr furrr
#' @export
#' @rdname collect
collect.disk.frame <- function(df, ..., parallel = F) {
  if(nchunks(df) > 0) {
    if(parallel) {
      furrr::future_map_dfr(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    } else {
      purrr::map_dfr(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    }
  } else {
    data.table()
  }
}

#' Bring the disk.frame into R as list
#' @import purrr furrr
#' @export
#' @rdname collect
collect_list <- function(df, ... , simplify = F, parallel = F) {
  if(nchunks(df) > 0) {
    res <- NULL
    if (parallel) {
      res = furrr::future_map(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    } else {
      res = purrr::map(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    }
    if (simplify) {
      return(simplify2array(res))
    } else {
      return(res)
    }
  } else {
    list()
  }
}