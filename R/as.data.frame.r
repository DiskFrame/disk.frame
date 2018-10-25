#' @export
as.data.frame.disk.frame <- function(df) {
  collect(df)
}

#' @export
as.data.table.disk.frame <- function(df) {
  setDT(collect(df))
}