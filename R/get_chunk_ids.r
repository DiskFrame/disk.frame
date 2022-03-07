#' Get the chunk IDs and files names
#' @param df a disk.frame
#' @param full.names If TRUE returns the full path to the file, Defaults to FALSE
#' @param strip_extension If TRUE then the file extension in the chunk_id is removed. Defaults to TRUE
#' @param ... passed to list.files
#' @importFrom stringr fixed
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # return the integer-string chunk IDs
#' get_chunk_ids(cars.df)
#' 
#' # return the file name chunk IDs
#' get_chunk_ids(cars.df, full.names = TRUE)
#' 
#' # return the file name chunk IDs with file extension
#' get_chunk_ids(cars.df, strip_extension = FALSE)
#' 
#' # clean up cars.df
#' delete(cars.df)
get_chunk_ids <- function(df, ..., full.names = FALSE, strip_extension = TRUE) {
  stopifnot("disk.frame" %in% class(df))
  
  lf = list.files(attr(df,"path"), full.names = full.names, ..., recursive = TRUE)
  if(full.names) {
    return(lf)
  }
  
  # strip out the path or file name if required
  sapply(lf, function(path) {
    tmp = basename(path)
    if (strip_extension) {
      tmp = tools::file_path_sans_ext(tmp)
    }
    return(tmp)
  })
}
