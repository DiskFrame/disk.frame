.onLoad <- function(libname, pkgname){
  print("Setting up disk.frame")
  setup_disk.frame()
}

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL