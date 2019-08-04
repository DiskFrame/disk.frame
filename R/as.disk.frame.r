#' Make a data.frame into a disk.frame
#' @param df a disk.frame
#' @param outdir the output directory
#' @param nchunks number of chunks
#' @param overwrite if TRUE the outdir will be overwritten, if FALSE it will throw an error if the directory is not empty
#' @param compress the compression level 0-100; 100 is highest
#' @param ... passed to output_disk.frame
#' @import fst
#' @importFrom data.table setDT
#' @export
as.disk.frame <- function(df, outdir, nchunks = recommend_nchunks(df), overwrite = FALSE, compress = 50, ...) {
  stopifnot("data.frame" %in% class(df))
  overwrite_check(outdir, overwrite)
  
  setDT(df)
  
  odfi = rep(1:nchunks, each = ceiling(nrow(df)/nchunks))
  odfi = odfi[1:nrow(df)]
  df[, .out.disk.frame.id := odfi]
  
  write_disk.frame(df, outdir, nchunks, overwrite, shardby="", compress = compress, ...)
}