#' Perform the computation
#' @param x a disk.frame
#' @param outdir the output directory
#' @param name not used
#' @param overwrite if TRUE overwrite outdir if it's a disk.frame
#' @param ... not used
#' @export
#' @importFrom dplyr compute
compute.disk.frame <- function(x, name, outdir = tempfile("tmp_df_", fileext=".df"), overwrite = T, ...) {
  map.disk.frame(x, base::I, outdir = outdir, lazy = F, overwrite=overwrite)
}
