keep <- function(...) {
  UseMethod("keep")
}

keep.disk.frame <- function(df, selections) {
  stopifnot("disk.frame" %in% class(df))
  attr(df,"keep") = selections
  df
}
