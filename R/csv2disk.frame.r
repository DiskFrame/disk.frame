#' convert CSV file(s) to disk.frame format
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
#' @param ... passed to data.table::fread, disk.frame::as.disk.frame, disk.frame::shard
#' @export
csv_to_disk.frame <- function(infile, outdir, inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                              in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = T, header = T, ...) {
  overwrite_check(outdir, overwrite)
  # reading multiple files
  if(length(infile) > 1) {
    dotdotdot = list(...)
    origarg = list(inmapfn = inmapfn, nchunks = nchunks,
                   in_chunk_size = in_chunk_size, shardby = shardby, compress = compress, 
                   overwrite = T, header = header)
    dotdotdotorigarg = c(dotdotdot, origarg)
    
    outdf = furrr::future_imap(infile, ~{
      dotdotdotorigarg1 = c(dotdotdotorigarg, list(outdir = file.path(tempdir(), .y), infile=.x))
      
      do_call(csv_to_disk.frame, dotdotdotorigarg1)
    }) %>% rbindlist.disk.frame(outdir = outdir, by_chunk_id = T, compress = compress, overwrite = overwrite)
    return(outdf)
  } else { # reading one file
    l = length(list.files(outdir))
    if(is.null(shardby)) {
      if(is.null(in_chunk_size)) {
        a = as.disk.frame(inmapfn(data.table::fread(infile, header=header, ...)), outdir, compress=compress, nchunks = nchunks, overwrite = overwrite, ...)
        return(a)
      } else {
        outdf = disk.frame(outdir)
        i <- 0
        tmpdir1 = tempfile(pattern="df_tmp")
        fs::dir_create(tmpdir1)

        done = F
        skiprows = 0
        column_names = ""
        while(!done) {
          if (identical(column_names, "")) {
            tmpdt = inmapfn(data.table::fread(
              infile,
              skip = skiprows, nrows = in_chunk_size,...))
            column_names = names(tmpdt)
          } else {
            ddd = list(...)
            if ("col.names" %in% names(ddd)) {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=F, ...))
            } else {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=F, col.names = column_names, ...))
            }
          }
          
          i <- i + 1
          skiprows = skiprows + in_chunk_size + 
            # skips the header as well but only at the first chunk
            ifelse(i == 1 & header, 1, 0)
          rows <- tmpdt[,.N]
          if(rows < in_chunk_size) {
            done <- T
          }
          
          # add to chunk
          add_chunk(outdf, tmpdt)
          rm(tmpdt); gc()
        }
        
        print(glue("read {in_chunk_size*(i-1) + rows} rows from {infile}"))
        
        # remove the files
        fs::dir_delete(tmpdir1)
      }
    } else { # so shard by some element
      if(is.null(in_chunk_size)) {
        shard(inmapfn(data.table::fread(infile, header=header, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = overwrite, compress = compress,...)
      } else {
        i <- 0
        tmpdir1 = tempfile(pattern="df_tmp")
        fs::dir_create(tmpdir1)
        #print(tmpdir1)
        
        done = F
        skiprows = 0
        column_names = ""
        while(!done) {
          if (identical(column_names, "")) {
            tmpdt = inmapfn(data.table::fread(
              infile,
              skip = skiprows, nrows = in_chunk_size,...))
            column_names = names(tmpdt)
          } else {
            ddd = list(...)
            if ("col.names" %in% names(ddd)) {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=F, ...))
            } else {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=F, col.names = column_names, ...))
            }
          }
          
          i <- i + 1
          skiprows = skiprows + in_chunk_size + 
            # skips the header as well but only at the first chunk
            ifelse(i == 1 & header, 1, 0)
          rows <- tmpdt[,.N]
          if(rows < in_chunk_size) {
            done <- T
          }
          
          tmp.disk.frame = shard(
            tmpdt, 
            shardby = shardby, 
            nchunks = nchunks, 
            outdir = file.path(tmpdir1,i), 
            overwrite = T,
            compress = compress,...)
          rm(tmpdt); gc()
        }
        
        print(glue("read {in_chunk_size*(i-1) + rows} rows from {infile}"))
        #
        # do not run this in parallel as the level above this is likely in parallel
        system.time(
          fnl_out <- 
            rbindlist.disk.frame(
              lapply(
                list.files(
                  tmpdir1,full.names = T), disk.frame), 
              outdir = outdir, by_chunk_id = T, parallel=F, overwrite = overwrite, compress = compress))
        
        # remove the files
        fs::dir_delete(tmpdir1)
      }
    }
    df = disk.frame(outdir)
    df = add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
    df
  }
}
