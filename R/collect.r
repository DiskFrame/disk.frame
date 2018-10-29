#' Bring the disk.frame into R
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
