#' a new print method for disk.frame
#' @export
#' @import glue
print.disk.frame <- function(df) {
  a = paste(sep = "\n"
             ,glue::glue("path: \"{attr(df,'path')}\"")
             ,glue::glue("nchunks: {nchunk(df)}")
             ,glue::glue("nrow: {nrow(df)}")
  )
  cat(a)
}