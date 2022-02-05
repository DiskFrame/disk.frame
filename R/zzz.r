.onLoad <- function(libname, pkgname){
  
}

#' @importFrom future nbrOfWorkers
#' @importFrom crayon red blue green
.onAttach <- function(libname, pkgname) {
  #setup_disk.frame()
  
  packageStartupMessage(
  crayon::blue(
"\n\n Thank you for using {disk.frame}. However {disk.frame} has been soft-deprecated and I recommend users to swith over to using the {arrow} package for handling larger-than-RAM data. You can convert your existing disk.frames to the parquet format which {arrow} can use by using:\n\n
```
disk.frame::disk.frame_to_parquet(path.to.your.disk.frame, parquet_path)
````

Once done you can use {arrow}'s dataset features to manipulate the larger-than-RAM data using dplyr verbs. E.g.

```
dataset = arrow::open_dataset(parquet_path)

parquet_path |>
  mutate(...) |>
  group_by(...) |>
  summarize(...) |>
  collect(...)
```
"
  ),
  crayon::red(
    glue::glue(
      "\n\n## Message from disk.frame:
We have {future::nbrOfWorkers()} workers to use with disk.frame.
To change that, use setup_disk.frame(workers = n) or just setup_disk.frame() to use the defaults."
    )
  ),
  crayon::green(
    "\n\n
It is recommended that you run the following immediately to set up disk.frame with multiple workers in order to parallelize your operations:\n\n
```r
# this will set up disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)
```
\n\n"
  ))
}

globalVariables(c(
                  "sym", # from dplyr
                  "type", # used in bloomfilter
                  "size", # used in bloomfilter
                  "modification_time", # used in bloomfilter
                  "name", # used in gen_summ_code
                  "agg_expr", # used in gen_summ_code
                  "orig_code", # used in gen_summ_code
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
                  "yid",
                  "paths",
                  ".disk.frame.sub.path",
                  "fullpath",
                  ".check", 
                  "partition_path"
                  ))

#' @useDynLib disk.frame
#' @importFrom Rcpp evalCpp
#@exportPattern "^[[:alpha:]]+"
NULL