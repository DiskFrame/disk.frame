#' Column names for RStudio auto-complete
#' @description 
#' Returns the names of the columns. Needed for RStudio to complete variable
#' names
#' @param x a disk.frame
#' @importFrom dplyr tbl_vars
#' @export
tbl_vars.disk.frame <- function(x) {
  names.disk.frame(x)
}

#' @rdname tbl_vars.disk.frame
#' @importFrom dplyr group_vars
#' @export
group_vars.disk.frame <- function(x) {
  # this is not applicable for disk.frame
  NULL
}