#' convert a CSV file to disk.frame format
#' @import fs
#' @importFrom glue glue
#' @param infile The input CSV
#' @param outdir The directory to output the disk.frame to
#' @param inmapfn A function to be applied to the chunk read in from CSV before the chunk is being written out. Commonly used to perform simple transformations. Defaults to the identity function (ie. no transformation)
#' @param nchunks Number of chunks to output
#' @param in_chunk_size When reading in the file, how many lines to read in at once. This is different to nchunks which controls how many chunks are output
#' @param shardby The column(s) to shard the data by. For example suppose `shardby = c("col1","col2")`  then every row where the values `col1` and `col2` are the same will end up in the same chunk; this will allow merging by `col1` and `col2` to be more efficient
#' @param compress For fst backends it's a number between 0 and 100 where 100 is the highest compression ratio.
#' @param overwrite Whether to overwrite the existing directory
#' @param ... passed to data.table::fread, disk.frame::as.disk.frame, disk.frame::shard
#' @export
#csv_to_disk.frame <- function(infile, outdir, inmapfn = base::I, nchunks = recommend_nchunks(file.size(infile)), in_chunk_size = NULL, shardby = NULL, colClasses = NULL, col.names = NULL, sep = "auto", compress = 50, overwrite = T,...) {
csv_to_disk.frame <- function(infile, outdir, inmapfn = base::I, nchunks = recommend_nchunks(file.size(infile)), in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = T, ...) {
  ##browser
  overwrite_check(outdir, overwrite)
  
  
  l = length(list.files(outdir))
  if(is.null(shardby)) {
    #a = write_fst(inmapfn(data.table::fread(infile, colClasses=colClasses, col.names = col.names, ...)), file.path(outdir,paste0(l+1,".fst")),compress=compress,...)
    a = as.disk.frame(inmapfn(data.table::fread(infile, ...)), outdir, compress=compress, nchunks = nchunks, overwrite = overwrite, ...)
    return(a)
  } else { # so shard by some element
    if(is.null(in_chunk_size)) {
      #shard(inmapfn(data.table::fread(infile,colClasses = colClasses, col.names = col.names, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = T,compress=compress,...)
      shard(inmapfn(data.table::fread(infile, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = overwrite, compress = compress,...)
    } else {
      i <- 0
      tmpdir1 = tempfile(pattern="df_tmp")
      dir.create(tmpdir1)
      print(tmpdir1)
      
      done = F
      skiprows = 0
      while(!done) {
        tmpdt = inmapfn(data.table::fread(
          infile,
          #colClasses = colClasses, 
          #col.names = col.names, 
          skip = skiprows, nrows = in_chunk_size, ...))
        i <- i + 1
        skiprows = skiprows + in_chunk_size
        rows <- tmpdt[,.N]
        if(rows < in_chunk_size) {
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
      

      print(glue("read {rows} rows from {infile}"))
      
      # do not run this in parallel as the level above this is likely in parallel
      system.time(
        fnl_out <- 
          rbindlist.disk.frame(
            lapply(
              list.files(
                tmpdir1,full.names = T), disk.frame), 
            outdir = outdir, by_chunk_id = T, parallel=F, overwrite = overwrite))
      
      # remove the files
      fs::dir_delete(tmpdir1)
      #unlink(tmpdir1, recursive = T, force = T)
    }
  }
  #disk.frame(outdir)
  df = disk.frame(outdir)
  add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
}

