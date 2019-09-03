#' Print disk.frame
#' @description
#' a new print method for disk.frame
#' @export
#' @param x disk.frame
#' @param ... not used
#' @importFrom glue glue
# TODO add chunk
print.disk.frame <- function(x, ...) {
  a = paste(sep = "\n"
             ,glue::glue("path: \"{attr(x,'path')}\"")
             ,glue::glue("nchunks: {disk.frame::nchunks(x)}")
             ,glue::glue("nrow (at source): {disk.frame::nrow(x)}")
             ,glue::glue("ncol (at source): {disk.frame::ncol(x)}")
             ,glue::glue("nrow (post operations): ???")
             ,glue::glue("ncol (post operations): ???\n")
  )
  message(a)
}
