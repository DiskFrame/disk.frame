# .onLoad <- function(libname, pkgname){
#   library(future)
#   library(future.apply)
#   nworkers = parallel::detectCores(logical=F)
#   plan(multiprocess, workers = nworkers, gc = T)
#   options(future.globals.maxSize=Inf)
#   options(disk.frame.nworkers = nworkers)
# }

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL