#' Write out a disk.frame to a location
#' @param df a disk.frame
#' @param outdir output directory for the disk.frame
#' @import fst
#' @export
write_disk.frame <- function(df, outdir, ..., overwrite = T) {
  if(is.null(outdir)) {
    stop("outdir must not be NULL")
  }
  map.disk.frame(df, ~.x, outdir = outdir, lazy = F, ..., overwrite = overwrite)
}