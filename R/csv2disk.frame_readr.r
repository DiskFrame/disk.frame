#@importFrom readr DataFrameCallback
#' @noRd
#' @noMd
csv_to_disk.frame_readr <- function(infile, outdir = tempfile(fileext = ".df"), inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                              in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = TRUE, col_names = TRUE, .progress = TRUE, delim=",", ...) {
  overwrite_check(outdir, overwrite)
  #browser()
  df = disk.frame(outdir)
  
  # TODO check header
  if(is.null(in_chunk_size)) {
    if(is.null(shardby)) {
      return(
        as.disk.frame(
          readr::read_delim(infile, delim = delim, ...) %>% inmapfn, 
          outdir = outdir, 
          overwrite = TRUE, 
          nchunks = nchunks, 
          compress = compress,
          ...
        )
      )
    } else {
      return(shard(readr::read_delim(infile, delim = delim, ...), shardby = shardby, outdir = outdir, nchunks = nchunks, overwrite = TRUE,...))
    }
  } else {
    if(!is.null(shardby)) {
      tmp_dir = tempfile()
      overwrite_check(tmp_dir, TRUE)
      df_tmp = disk.frame(tmp_dir)
      f = function(x, pos) {
        add_chunk(df_tmp, inmapfn(x))
      }
      
      message("csv_to_disk.frame reader backend: Stage 1/1 -- reading file")
      readr::read_delim_chunked(infile, readr::SideEffectChunkCallback$new(f), chunk_size = in_chunk_size,  delim = delim, ...)
      
      message(glue::glue("csv_to_disk.frame reader backend: Stage 2/2 -- performing shardby {shardby}"))
      df = shard(df_tmp,shardby = shardby,outdir = outdir, nchunks = nchunks, overwrite = TRUE)
      df = add_meta(df_tmp, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
      df
    } else {
      f = function(x, pos) {
        add_chunk(df, inmapfn(x))
      }
      readr::read_delim_chunked(infile, readr::SideEffectChunkCallback$new(f), chunk_size = in_chunk_size,  delim = delim, ...)
      df = add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
      df
    }
  }
}