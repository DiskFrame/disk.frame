#' Return the column names of the disk.frame
#'
#' The returned column names are from the source. So if you have lazy operations
#' then the \code{colnames} here does not reflects the results of those
#' operations. Note: if you have expensive lazy function then this operation
#' might take some time.
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
  if(is.null(attr(x, "recordings"))) {
    if(length(res) == 0) {
      return(vector("character"))
    }
    return(fst::metadata_fst(res[1])$columnNames)
  } else {
    tiny_example_data.frame = get_chunk(x, 1, from=1, to=1)
    return(colnames(tiny_example_data.frame))
  }
}


#' @rdname colnames
#' @export
colnames.default <- function(x, ...) {
  base::colnames(x, ...)
}
