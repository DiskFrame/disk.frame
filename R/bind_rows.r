#' Bind rows
#' @param ... 
#' @export
bind_rows.disk.frame <- function(...) {
  rbindlist.disk.frame(list(...))
}