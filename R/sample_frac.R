#' Sample n rows from a disk.frame
#' @param df a disk.frame
#' @param size the proportion/fraction of rows to sample
#' @param replace TRUE to sample with replacement; FALSE to sample without replacement
#' @param weight weight of each row. NOT implemented
#' @import dplyr
#' @export
#' @rdname sample
sample_frac.disk.frame <- function(df, size = 1, replace = FALSE, weight = NULL, .env = NULL) {
  if(!is.null(weight)) {
    stop("sample_frac(..., weight =) is not implemented yet")
  }
  
  delayed(df, ~sample_frac(.x, size, replace, weight, .env))
}

