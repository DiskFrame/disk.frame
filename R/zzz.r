.onLoad <- function(libname, pkgname){
  
}

#' @importFrom future nbrOfWorkers
.onAttach <- function(libname, pkgname) {
  #setup_disk.frame()
  
  packageStartupMessage(
    glue::glue("We have {future::nbrOfWorkers()} workers to use with disk.frame. To change that use setup_disk.frame(workers = n) or just setup_disk.frame() to use the defaults"))
}

globalVariables(c(
                  "syms", # needed by dplyr to treat something as a symbol
                  ".",
                  ".BY",
                  ".N",
                  ".SD",
                  ".out.disk.frame.id",
                  ":=",
                  "N",
                  "area",
                  "chunk_id",
                  "coltypes",
                  "coltypes.x",
                  "coltypes.y",
                  "ctot",
                  "existing_df",
                  "feature_s",
                  "h",
                  "height",
                  "incompatible_types",
                  "lag_height",
                  "new_chunk",
                  "ok",
                  "pathA",
                  "pathB",
                  "w",
                  "xid",
                  "yid"))

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL