#' Sample n rows from a disk.frame
#' @param tbl a disk.frame
#' @param size the proportion/fraction of rows to sample
#' @param replace TRUE to sample with replacement; FALSE to sample without replacement
#' @param weight weight of each row. NOT implemented
#' @param .env for compatibility
#' @param ... passed to dplyr
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

