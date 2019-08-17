#' Delete a disk.frame
#' @param df a disk.frame
#' @importFrom fs dir_delete
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' delete(cars.df)
delete <- function(df) {
  stopifnot("disk.frame" %in% class(df))
  
  fs::dir_delete(attr(df, "path"))
}