#' Sample n rows from a disk.frame
#' @param tbl a disk.frame
#' @param size the proportion/fraction of rows to sample
#' @param replace TRUE to sample with replacement; FALSE to sample without replacement
#' @param weight weight of each row. NOT implemented
#' @param .env for compatibility
#' @import dplyr
#' @export
#' @rdname sample
sample_frac.disk.frame <- function(tbl, size = 1, replace = FALSE, weight = NULL, .env = NULL) {
  if(!is.null(weight)) {
    stop("sample_frac(..., weight =) is not implemented yet")
  }
  
  delayed(tbl, ~sample_frac(.x, size, replace, weight, .env))
}

