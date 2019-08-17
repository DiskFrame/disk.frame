#' Number of rows or columns
#' @param ... passed to base::nrow
#' @export
#' @rdname ncol_nrow
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # return total number of column and rows
#' ncol(cars.df)
#' nrow(cars.df)
#' 
#' # clean up cars.df
#' delete(cars.df)
nrow <- function(df,...) {
  UseMethod("nrow")
}

#' @rdname ncol_nrow
#' @export
nrow.default <- function(df, ...) {
  base::nrow(df, ...)
}

#' @export
#' @rdname ncol_nrow
#' @import fst
nrow.disk.frame <- function(df, ...) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- list.files(path1,full.names = TRUE)
    if(length(path2) == 0) {
      return(0)
    }
    tmpfstmeta = fst::fst.metadata(path2[1])
    if("nrOfRows" %in% names(tmpfstmeta)) {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$nrOfRows)))
    } else {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$NrOfRows)))
    }
  } else {
    #return(fst::fst.metadata(path1)$NrOfRows)
    stop(glue::glue("nrow error: directory {} does not exist"))
  }
}

#' @import fst
#' @export
#' @rdname ncol_nrow
ncol <- function(df) {
  UseMethod("ncol")
}

#' @import fs
#' @export
#' @param df a disk.frame
#' @rdname ncol_nrow
ncol.disk.frame <- function(df) {
  length(colnames(df))
}

#' @export
ncol.default <- function(df) {
  base::ncol(df)
}