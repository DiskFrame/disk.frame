#' @export
#' @import fs
#' @import fst
names.disk.frame <- function(df) {
  res = attr(df,"path") %>% 
    fs::dir_ls(type="file")
  if(length(res) == 0) {
    return(vector("character"))
  }
  fst::metadata_fst(res[1])$columnNames
}

#' @param df a disk.frame
#' @rdname colnames
colnames.disk.frame <- function(df) {
  names.disk.frame(df)
}

#' Return the column names of the disk.frame
#' @param ... passed to colnames
#' @export
colnames <- function(x, ...) {
  UseMethod("colnames")
}

#' @rdname colnames
colnames.default <- function(df, ...) {
  Base::colnames(df, ...)
}