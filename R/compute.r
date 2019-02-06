#' Perform the computation
#' @param df a disk.frame
#' @param outdir the output directory
#' @param overwrite if TRUE overwrite outdir if it's a disk.frame
#' @export
compute.disk.frame <- function(df, outdir, overwrite = T) {
  ##browser
  map.disk.frame(df, base::I, outdir = outdir, lazy = F, overwrite=overwrite)
}
