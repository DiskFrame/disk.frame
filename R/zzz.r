.onLoad <- function(libname, pkgname){
  packageStartupMessage("Setting up disk.frame")
  setup_disk.frame()
}

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL