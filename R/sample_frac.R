#' Sample n rows from a disk.frame
#' @param .data a disk.frame
#' @param ... passed to dplyr::samle_frac
#' @export
#' @importFrom dplyr sample_frac
#' @rdname sample
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' collect(sample_frac(cars.df, 0.5))
#' 
#' # clean up cars.df
#' delete(cars.df)
sample_frac.disk.frame <- create_dplyr_mapper(dplyr::sample_frac, warning_msg = "sample_frac: for disk.frames weight = is not supported")

