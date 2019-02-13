#' Get the chunk file names
#' @param df a disk.frame
#' @param full.names If TRUE returns the full path to the file, Defaults to F.
#' @param strip_extension If TRUE then the file extentsion in the chunk_id is removed. Defaults to TRUE
#' @param ... passed to list.files
#' @import stringr
get_chunk_ids <- function(df, ..., full.names = F, strip_extension = T) {
  lf = list.files(attr(df,"path"), full.names = full.names, ...)
  if(full.names) {
    return(lf)
  }
  purrr::map_chr(lf, ~{
    tmp = stringr::str_split(.x,stringr::fixed("."), simplify = T)
    l = length(tmp)
    if(l == 1) {
      return(tmp)
    } else if(strip_extension) {
      paste0(tmp[-l], collapse="")
    } else if (l==1) {
      paste0(tmp[-l], collapse="")
    }
  })
}
