#' Return the column names of the disk.frame
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
  res = attr(x, "path") %>% 
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