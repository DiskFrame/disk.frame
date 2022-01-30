#' Bind rows
#' @param ... disk.frame to be row bound
#' @export
bind_rows.disk.frame <- function(...) {
  rbindlist.disk.frame(list(...))
}