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
  lf = list.files(attr(df,"path"), full.names = full.names, ...)
  if(full.names) {
    return(lf)
  }
  purrr::map_chr(lf, ~{
    tmp = stringr::str_split(.x,stringr::fixed("."), simplify = TRUE)
    l = length(tmp)
    if(l == 1) {
      return(tmp)
    } else if(strip_extension) {
      paste0(tmp[-l], collapse="")
    } else {
      .x
    }
  })
}
