#' Return the column names of the disk.frame
#' 
#' The returned column names are from the source. So if you have lazy operations then the 
#' colnames here does not reflects the results of those operations. To obtain the correct names try
#' \code{names(collect(get_chunk(df, 1)))}
#' 
#' @param x a disk.frame
#' @param ... not used
#' @export
#' @importFrom fs dir_ls
#' @importFrom fst metadata_fst
#' @export
colnames <- function(x, ...) {
  UseMethod("colnames")
}

#' @rdname colnames
#' @export
names.disk.frame <- function(x, ...) {
  colnames.disk.frame(x, ...)
}

#' @rdname colnames
#' @export
colnames.disk.frame <- function(x, ...) {
  res = attr(x, "path", exact=TRUE) %>% 
    fs::dir_ls(type="file")
  if(length(res) == 0) {
    return(vector("character"))
  }
  fst::metadata_fst(res[1])$columnNames
}


#' @rdname colnames
#' @export
colnames.default <- function(x, ...) {
  base::colnames(x, ...)
}