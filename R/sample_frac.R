#' Sample n rows from a disk.frame
#' @export
#' @importFrom dplyr sample_frac
#' @inheritParams dplyr::sample_frac
#' @rdname sample
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' collect(sample_frac(cars.df, 0.5))
#' 
#' # clean up cars.df
#' delete(cars.df)
sample_frac.disk.frame <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL, ...) {
  warning_msg = NULL
  if(!is.null(weight)) {
    warning_msg = "sample_frac: for disk.frames weight = is not supported"
    stop(warning_msg)
  } else if (!is.null(.env)) {
    warning_msg = "sample_frac: for disk.frames .env = is not supported"
    stop(warning_msg)
  }
  
  fn = disk.frame::create_chunk_mapper(dplyr::sample_frac)
  
  fn(tbl, size = size, replace = replace, ...)
}

