#' convert a CSV file to disk.frame format
#' @import glue fs
#' @param infile The input CSV
#' @param outdir The directory to output the disk.frame to
#' @param inmapfn A function to be applied to the chunk read in from CSV before the chunk is being written out. Commonly used to perform simple transformtions. Defaults to the identity function (ie. no transformation)
#' @param nchunks Number of chunks to output
#' @param in_chunk_size When reading in the file, how many lines to read in at once. This is different to nchunks which controls how many chunks are output
#' @param shardby The column(s) to shard the data by. For example suppose `shardby = c("col1","col2")`  then every row where the values `col1` and `col2` are the same will end up in the same chunk; this will allow merging by `col1` and `col2` to be more efficient
#' @param colClasses A vector containing the class of the columns. Valid values are "c", "n", "i", "d" for character, numeric, integer, and date respectively.
#' @param col.names The names of the columns
#' @param sep The delimiter of the CSV fle, otherwise known as the separator
#' @param compress For fst backends it's a number between 0 and 100 where 100 is the highest compression ratio.
#' @export
csv_to_disk.frame <- function(infile, outdir, inmapfn = base::I, nchunks = recommend_nchunks(file.size(infile)), in_chunk_size = NULL, shardby = NULL, colClasses = NULL, col.names = NULL, sep = "auto", compress=50,...) {
#csv_to_disk.frame <- function(infile, outdir, inmapfn = base::I, nchunks = recommend_nchunks(file.size(infile)), in_chunk_size = NULL, shardby = NULL, compress=50, ...) {
  #browser()
  #fs::dir_create(outdir)
  
  l = length(list.files(outdir))
  if(is.null(shardby)) {
    #a = write_fst(inmapfn(fread(infile, colClasses=colClasses, col.names = col.names, ...)), file.path(outdir,paste0(l+1,".fst")),compress=compress,...)
    a = as.disk.frame(inmapfn(fread(infile, ...)), outdir, compress=compress, nchunks = nchunks, overwrite = T, ...)
    return(a)
  } else { # so shard by some element
    #browser()
    if(is.null(in_chunk_size)) {
      #shard(inmapfn(fread(infile,colClasses = colClasses, col.names = col.names, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = T,compress=compress,...)
      shard(inmapfn(fread(infile, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = T, compress = compress,...)
    } else {
      i <- 0
      tmpdir1 = tempfile(pattern="df_tmp")
      dir.create(tmpdir1)
      print(tmpdir1)
      
      done = F
      skiprows = 0
      while(!done) {
        tmpdt = inmapfn(fread(
          infile,
          #colClasses = colClasses, 
          #col.names = col.names, 
          skip = skiprows, nrows = in_chunk_size, ...))
        i <- i + 1
        skiprows = skiprows + in_chunk_size
        rows <- tmpdt[,.N]
        if(rows < in_chunk_size) {
          #browser()
          done <- T
        }
        
        shard(
          tmpdt, 
          shardby = shardby, 
          nchunks = nchunks, 
          outdir = file.path(tmpdir1,i), 
          overwrite = T,
          compress = compress,...)
        rm(tmpdt); gc()
      }
      
      
      # readr::read_delim_chunked(infile, delim = sep, chunk_size = in_chunk_size, col_names = col.names, col_types = ct, function(indf,...) {
      #   i <<- i + 1
      #   shard(indf, shardby = shardby, nchunks = nchunks, outdir = file.path(tmpdir1,i), overwrite = T, compress=compress,...)
      # })
      #browser()
      print(glue("read {rows} rows from {infile}"))
      
      # do not run this in parallel as the level above this is likely in parallel
      system.time(fnl_out <- rbindlist.disk.frame(lapply(list.files(tmpdir1,full.names = T), disk.frame), outdir = outdir, by_chunk_id = T, parallel=F))
      
      # remove the files
      unlink(tmpdir1, recursive = T, force = T)
      #browser()
    }
  }
  disk.frame(outdir)
}

