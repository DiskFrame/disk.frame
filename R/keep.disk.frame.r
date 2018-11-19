#' Keep only the variables from the input listed in selections
#' @param df a disk.frame
#' @param selections The list of variables to keep from the input source
#' @export
keep <- function(...) {
  UseMethod("keep")
}

#' @export
keep.disk.frame <- function(df, selections) {
  stopifnot("disk.frame" %in% class(df))
  attr(df,"keep") = selections
  df
}

#' @rdname keep
#' @export
srckeep <- function(...) {
  keep.disk.frame(...)
}
