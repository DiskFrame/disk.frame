.onLoad <- function(libname, pkgname){
  
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("Setting up disk.frame")
  setup_disk.frame()
}

globalVariables(c(".",
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