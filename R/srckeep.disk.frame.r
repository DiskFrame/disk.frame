#' Keep only the variables from the input listed in selections
#' @param df a disk.frame
#' @param selections The list of variables to keep from the input source
#' @export
srckeep <- function(df, selections) {
  stopifnot("disk.frame" %in% class(df))
  attr(df,"keep") = selections
  df
}
