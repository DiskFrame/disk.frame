#' Shard a data.frame/data.table into chunk and saves it into a disk.frame
#' @param df A disk.frame
#' @param shardby The column(s) to shard the data by.
#' @param nchunks The number of chunks
#' @param outdir The output directory of the disk.frame
#' @param overwrite If TRUE then the chunks are overwritten
#' @import glue fst
#' @export
shard <- function(df, shardby, outdir = tempfile("tmp_disk_frame_shard"), ..., nchunks = recommend_nchunks(df), overwrite = F) {
  overwrite_check(outdir, overwrite)
  
  setDT(df)
  if(length(shardby) == 1) {
    code = glue::glue("df[,.out.disk.frame.id := disk.frame:::hashstr2i(as.character({shardby}), nchunks)]")
  } else {
    shardby_list = glue::glue("paste0({paste0(shardby,collapse=',')})")
    code = glue::glue("df[,.out.disk.frame.id := disk.frame:::hashstr2i({shardby_list}, nchunks)]")
  }
  
  eval(parse(text=code))
  
  output_disk.frame(df, outdir, nchunks, overwrite, shardkey = shardby, shardchunks = nchunks)
}