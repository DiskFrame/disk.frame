#' delete a disk.frame
#' @param df a disk.frame
#' @importFrom fs dir_delete
#' @export
delete <- function(df) {
  stopifnot("disk.frame" %in% class(df))
  
  fs::dir_delete(attr(df, "path"))
}