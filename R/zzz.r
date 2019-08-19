.onLoad <- function(libname, pkgname){
  
}

#' @importFrom future nbrOfWorkers
#' @importFrom crayon red blue green
.onAttach <- function(libname, pkgname) {
  #setup_disk.frame()
  
  packageStartupMessage(
    crayon::red(
    glue::glue(
    "\n\n## Message from disk.frame:
We have {future::nbrOfWorkers()} workers to use with disk.frame.
To change that use setup_disk.frame(workers = n) or just setup_disk.frame() to use the defaults.")),
    crayon::green("\n\n
It is recommend that you run the following immediately to setup disk.frame with multiple workers in order to parallelize your operations:\n\n
```r
# this willl set disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)
```
\n\n"))
  
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