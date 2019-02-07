#' Convert disk.frame to data.frame by collecting all chunks
#' @param df a disk.frame
#' @export
as.data.frame.disk.frame <- function(x, row.names, optional, ...) {
  as.data.frame(collect(x), row.names, optional, ...)
}

#' Convert disk.frame to data.table by collecting all chunks
#' @rdname as.data.frame.disk.frame
#' @export
as.data.table.disk.frame <- function(df) {
  setDT(collect(df))
}