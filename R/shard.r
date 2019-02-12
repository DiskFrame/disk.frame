#' Shard a data.frame/data.table or disk.frame into chunk and saves it into a disk.frame
#' @param df A disk.frame
#' @param shardby The column(s) to shard the data by.
#' @param nchunks The number of chunks
#' @param outdir The output directory of the disk.frame
#' @param overwrite If TRUE then the chunks are overwritten
#' @param ... not used
#' @import fst
#' @importFrom glue glue
#' @export
shard <- function(df, shardby, outdir = tempfile("tmp_disk_frame_shard"), ..., nchunks = recommend_nchunks(df), overwrite = F) {
  ##browser
  overwrite_check(outdir, overwrite)
  
  setDT(df)
  if(length(shardby) == 1) {
    code = glue::glue("df[,.out.disk.frame.id := hashstr2i(as.character({shardby}), nchunks)]")
  } else {
    shardby_list = glue::glue("paste0({paste0(shardby,collapse=',')})")
    code = glue::glue("df[,.out.disk.frame.id := hashstr2i({shardby_list}, nchunks)]")
  }
  
  eval(parse(text=code))
  
  write_disk.frame(df, outdir = outdir, nchunks = nchunks, overwrite = overwrite, shardkey = shardby, shardchunks = nchunks)
}

#' `distribute` is an alias for `shard`
#' @export
#' @rdname shard
distribute <- function(...) {
  UseMethod("shard")
}