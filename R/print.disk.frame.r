#' a new print method for disk.frame
#' @export
#' @param x disk.frame
#' @param ... not used
#' @importFrom glue glue
print.disk.frame <- function(x, ...) {
  a = paste(sep = "\n"
             ,glue::glue("path: \"{attr(x,'path')}\"")
             ,glue::glue("nchunks: {disk.frame::nchunks(x)}")
             ,glue::glue("nrow: {disk.frame::nrow(x)}")
             ,glue::glue("ncol: {disk.frame::ncol(x)}")
  )
  cat(a)
}
