#' Keep only the variables from the input listed in selections
#' @param diskf a disk.frame
#' @param selections The list of variables to keep from the input source
#' @param ... not yet used
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # when loading cars's chunks into RAM, load only the column speed
#' collect(srckeep(cars.df, "speed"))
#' 
#' # clean up cars.df
#' delete(cars.df)
srckeep <- function(diskf, selections, ...) {
  stopifnot("disk.frame" %in% class(diskf))
  attr(diskf,"keep") = selections
  
  diskf
}

#' @param chunks The chunks to load
#' @rdname srckeep
#' @export
srckeepchunks <- function(df, chunks, ...) {
  stopifnot("disk.frame" %in% class(df))
  # TODO relax this
  stopifnot(is.integer(chunks))
  
  attr(df,"keep_chunks") = chunks
  df
}
