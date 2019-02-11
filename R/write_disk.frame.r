#' Write out a data.frame/disk.frame to a disk.frame location
#' @param df a disk.frame
#' @param outdir output directory for the disk.frame
#' @param nchunks number of chunks
#' @param overwrite overwrite output directory
#' @param shardkey the columns to shard by
#' @param shardchunks the number of chunks to shard to shard to. It is always greater than or equal to nchunks
#' @param compress compression ratio for fst files
#' @param ... passed to map.disk.frame
#' @export
#' @import fst fs
#' @importFrom glue glue
write_disk.frame <- function(df, outdir, nchunks, overwrite, shardkey, shardchunks, compress = 50, ...) {
  ##browser
  overwrite_check(outdir, overwrite)

  if(is.null(outdir)) {
    stop("outdir must not be NULL")
  }
  
  if(is_disk.frame(df)) {
    map.disk.frame(df, ~.x, outdir = outdir, lazy = F, ..., overwrite = overwrite)
  } else if ("data.frame" %in% class(df)) {
    df[,{
      if (base::nrow(.SD) > 0) {
        fst::write_fst(.SD, file.path(outdir, paste0(.BY, ".fst")), compress = compress)
        NULL
      }
      NULL
    }, .out.disk.frame.id]
    res = disk.frame(outdir)
    add_meta(res, shardkey = shardkey, shardchunks = shardchunks, compress = compress)
  } else {
    stop("write_disk.frame error: df must be a disk.frame or data.frame")
  }
}

#' @rdname write_disk.frame
output_disk.frame <- function(...) {
  warning("output_disk.frame is DEPRECATED. Use write_disk.frame istead")
  write_disk.frame(...)
}