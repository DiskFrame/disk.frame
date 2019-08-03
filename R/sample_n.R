sample_n.disk.frame <- function(df, size = 1, replace = FALSE, weight = NULL, .env = NULL){
  stop("not implemented yet")
  if(!is.null(weight)) {
    stop("sample_n(..., weight =) is not implemented yet")
  }

  #delayed(df, ~sample_frac(.x, size, replace, weight, .env))
}

