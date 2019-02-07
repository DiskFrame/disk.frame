#' @export
#' @import fs
#' @import fst
#' @rdname colnames
names.disk.frame <- function(x, ...) {
  res = attr(x, "path") %>% 
    fs::dir_ls(type="file")
  if(length(res) == 0) {
    return(vector("character"))
  }
  fst::metadata_fst(res[1])$columnNames
}

#' @param x a disk.frame
#' @param ... not used
#' @rdname colnames
#' @export
colnames.disk.frame <- function(x, ...) {
  names.disk.frame(x, ...)
}

#' Return the column names of the disk.frame
#' @param ... passed to colnames
#' @export
colnames <- function(x, ...) {
  UseMethod("colnames")
}

#' @rdname colnames
#' @export
colnames.default <- function(x, ...) {
  Base::colnames(x, ...)
}