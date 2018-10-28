.onLoad <- function(libname, pkgname){
  library(future)
  library(future.apply)
  library(dplyr)
  library(data.table)
  library(dtplyr)
  nworkers = parallel::detectCores(logical=F)
  #nworkers = parallel::detectCores()
  plan(multiprocess, workers = nworkers, gc = T)
  options(future.globals.maxSize=Inf)
  options(disk.frame.nworkers = nworkers)
}

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL