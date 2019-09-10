#' @importFrom readr DataFrameCallback
#' @noRd
#' @noMd
csv_to_disk.frame_readr <- function(infile, outdir = tempfile(fileext = ".df"), inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                              in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = TRUE, header = TRUE, .progress = TRUE, delim=",", ...) {
  overwrite_check(outdir, overwrite)
  
  df = disk.frame(outdir)
  
  f = function(x, pos) {
    add_chunk(df, x)
  }
  
  readr::read_delim_chunked(infile, readr::DataFrameCallback$new(f), chunk_size = in_chunk_size,  delim = delim, ...)
  
  df = add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
  df
}