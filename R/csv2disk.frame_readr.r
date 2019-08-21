#' Convert CSV file(s) to disk.frame format
#' @importFrom glue glue
#' @importFrom fs dir_delete
#' @importFrom pryr do_call
#' @param infile The input CSV file or files
#' @param outdir The directory to output the disk.frame to
#' @param inmapfn A function to be applied to the chunk read in from CSV before the chunk is being written out. Commonly used to perform simple transformations. Defaults to the identity function (ie. no transformation)
#' @param nchunks Number of chunks to output
#' @param in_chunk_size When reading in the file, how many lines to read in at once. This is different to nchunks which controls how many chunks are output
#' @param shardby The column(s) to shard the data by. For example suppose `shardby = c("col1","col2")`  then every row where the values `col1` and `col2` are the same will end up in the same chunk; this will allow merging by `col1` and `col2` to be more efficient
#' @param compress For fst backends it's a number between 0 and 100 where 100 is the highest compression ratio.
#' @param overwrite Whether to overwrite the existing directory
#' @param header Whether the files have header. Defaults to TRUE
#' @param .progress A logical, for whether or not to print a progress bar for multiprocess, multisession, and multicore plans. From {furrr}
#' @param ... passed to data.table::fread, disk.frame::as.disk.frame, disk.frame::shard
#' @importFrom pryr do_call
#' @export
#' @examples 
#' tmpfile = tempfile()
#' write.csv(cars, tmpfile)
#' tmpdf = tempfile(fileext = ".df")
#' df = csv_to_disk.frame(tmpfile, outdir = tmpdf, overwrite = TRUE)
#' 
#' # clean up
#' fs::file_delete(tmpfile)
#' delete(df)
csv_to_disk.frame_readr <- function(infile, outdir = tempfile(fileext = ".df"), inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                              in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = TRUE, header = TRUE, .progress = TRUE, ...) {
  overwrite_check(outdir, overwrite)
  
  df = disk.frame(outdir)
  
  readr::read_delim_chunked(infile, function(chunk,i) {
    add_chunk(df, chunk)
  }, chunk_size = in_chunk_size, ..., delim="|")
  
  df = add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
  df
}