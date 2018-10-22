keep <- function(...) {
  UseMethod("keep")
}

keep.disk.frame <- function(df, selections) {
  stopifnot("disk.frame" %in% class(df))
  attr(df1,"keep") = selections
  df1
}
