#' Convert disk.frame to data.frame by collecting all chunks
#' @export
as.data.frame.disk.frame <- function(df) {
  collect(df)
}

#' Convert disk.frame to data.table by collecting all chunks
#' @export
as.data.table.disk.frame <- function(df) {
  setDT(collect(df))
}