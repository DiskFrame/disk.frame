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

colnames.disk.frame <- function(df) {
  names.disk.frame(df)
}

#' @export
colnames <- function(x, ...) {
  UseMethod("colnames")
}

colnames.default <- function(x, ...) {
  Base::colnames(x, ...)
}