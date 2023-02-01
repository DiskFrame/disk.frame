#' Convert disk.frame to data.frame by collecting all chunks
#' @param x a disk.frame
#' @param row.names NULL or a character vector giving the row names for the data frame. Missing values are not allowed.
#' @param optional logical. If TRUE, setting row names and converting column names (to syntactic names: see make.names) is optional. Note that all of R's base package as.data.frame() methods use optional only for column names treatment, basically with the meaning of data.frame(*, check.names = !optional). See also the make.names argument of the matrix method.
#' @param ... additional arguments to be passed to or from methods.
#' @export
#' @examples 
#' cars.df = as.disk.frame(cars)
#' as.data.frame(cars.df)
#' 
#' # clean up
#' delete(cars.df)
as.data.frame.disk.frame <- function(x, row.names = NULL, optional = FALSE, ...) { # needs to retain x for consistency
  as.data.frame(collect(x), row.names, optional, ...)
}

#' Convert disk.frame to data.table by collecting all chunks
#' @param x a disk.frame
#' @param keep.rownames passed to as.data.table
#' @param ... passed to as.data.table
#' @export
#' @examples 
#' library(data.table)
#' cars.df = as.disk.frame(cars)
#' as.data.table(cars.df)
#' 
#' # clean up
#' delete(cars.df)
as.data.table.disk.frame <- function(x, keep.rownames = FALSE, ...) {
  as.data.table(collect(x), keep.rownames = keep.rownames, ...)
}