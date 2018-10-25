.onLoad <- function(libname, pkgname){
  library(future)
  
  #nworkers = parallel::detectCores(logical=F)
  nworkers = parallel::detectCores()
  plan(multiprocess, workers = nworkers, gc = T)
  options(future.globals.maxSize=Inf)
  options(disk.frame.nworkers = nworkers)
}

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#' @exportPattern "^[[:alpha:]]+"
NULL