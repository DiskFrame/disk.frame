#' convert a CSV file to disk.frame format
#' @import glue
#' @param in_chunk_size When reading in the file, how many chunks to read it in. This is different to nchunks which controls how many chunks are output
csv_to_disk.frame <- function(infile, outdir, inmapfn = function(x) x, nchunks = 16, in_chunk_size = NULL, append = F, shardby = NULL, colClasses = NULL, col.names = NULL, sep = "auto", compress=50,...) {
  #browser()
  if(!dir.exists(outdir)) {
    dir.create(outdir)
  } else if ((!append) & length(dir(outdir)) >0 ) {
    stop(glue("append is FALSE and the directory '{outdir}' is not empty"))
  } else if(length(dir(outdir)) == 0) {
    # the folder is empty just load it
  }
  
  l = length(dir(outdir))
  if(is.null(shardby)) {
    a = write_fst(inmapfn(fread(infile,colClasses=colClasses, col.names = col.names, ...)), file.path(outdir,paste0(l+1,".fst")),compress=compress,...)
    rm(a); gc()
  } else { # so shard by some element
    #browser()
    if(is.null(in_chunk_size)) {
      shard(inmapfn(fread(infile,colClasses = colClasses, col.names = col.names, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = T,compress=compress,...)
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
          colClasses = colClasses, 
          col.names = col.names, skip = skiprows, nrows = in_chunk_size, ...))
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
      print(rows)
      
      # do not run this in parallel as the level above this is likely in parallel
      system.time(fnl_out <- rbindlist.disk.frame(lapply(dir(tmpdir1,full.names = T), disk.frame), outdir = outdir, by_chunk_id = T, parallel=F))
      
      # remove the files
      unlink(tmpdir1,recursive = T, force = T)
      #browser()
    }
  }
  disk.frame(outdir)
}

