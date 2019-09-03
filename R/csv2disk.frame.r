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
csv_to_disk.frame <- function(infile, outdir = tempfile(fileext = ".df"), inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                              in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = TRUE, header = TRUE, .progress = TRUE, backend = "data.table", strategy = c("data.table", "readLines"), ...) {
  overwrite_check(outdir, overwrite)
  strategy = match.arg(strategy)
  if(backend == "data.table" & strategy == "data.table") {
    csv_to_disk.frame_data.table_backend(infile, outdir, inmapfn, nchunks, in_chunk_size, shardby, compress, overwrite, header, .progress, ...)
  } else if (backend == "data.table" & strategy == "readLines" & !is.null(in_chunk_size)) {
    if (length(infile) == 1) {
      # establish a read connection to the file
      con = file(infile, "r")
      on.exit(close(con))
      xx = readLines(con, n = in_chunk_size)
      diskf = disk.frame(outdir)
      header_copy = header
      while(length(xx) >= in_chunk_size) {
        add_chunk(diskf, inmapfn(data.table::fread(text = xx, header = header_copy, ...)))
        xx = readLines(con, n = in_chunk_size)
        header_copy = FALSE
      }
      return(diskf)
    } else {
      stop("strategy = 'readLines' is not yet supported for multiple files")
    }
  } else {
    stop("csv_to_disk.frame: this set of options is not supported")
  }
}


csv_to_disk.frame_data.table_backend <- function(infile, outdir = tempfile(fileext = ".df"), inmapfn = base::I, nchunks = recommend_nchunks(sum(file.size(infile))), 
                                                 in_chunk_size = NULL, shardby = NULL, compress=50, overwrite = TRUE, header = TRUE, .progress = TRUE, ...) {
  # reading multiple files
  if(length(infile) > 1) {
    dotdotdot = list(...)
    origarg = list(inmapfn = inmapfn, nchunks = nchunks,
                   in_chunk_size = in_chunk_size, shardby = shardby, compress = compress, 
                   overwrite = TRUE, header = header)
    dotdotdotorigarg = c(dotdotdot, origarg)
    
    pt <- proc.time()
    if(.progress) {
      message("-- Converting CSVs to disk.frame --")
      
      message(glue::glue("Converting {length(infile)} CSVs to {nchunks} disk.frame each consisting of {nchunks} chunks (Stage 1 of 2):"))
    }
    
    outdf_tmp = furrr::future_imap(infile, ~{
      dotdotdotorigarg1 = c(dotdotdotorigarg, list(outdir = file.path(tempdir(), .y), infile=.x))
      
      pryr::do_call(csv_to_disk.frame_data.table_backend, dotdotdotorigarg1)
    }, .progress = .progress)
    
    if(.progress) {
      message(paste("Stage 1 or 2 took:", data.table::timetaken(pt)))
      message(" ")
    }
    
    message(glue::glue("Row-binding the {nchunks} disk.frames together to form one large disk.frame (Stage 2 of 2):"))
    message(glue::glue("Creating the disk.frame at {outdir}"))
    pt2 <- proc.time()
    outdf = rbindlist.disk.frame(outdf_tmp, outdir = outdir, by_chunk_id = TRUE, compress = compress, overwrite = overwrite, .progress = .progress)
    
    if(.progress) {
      
      message(paste("Stage 2 or 2 took:", data.table::timetaken(pt2)))
      message(" ----------------------------------------------------- ")
      message(paste("Stage 1 & 2 in total took:", data.table::timetaken(pt)))
    }
    
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
        
        done = FALSE
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
                header=FALSE, ...))
            } else {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=FALSE, col.names = column_names, ...))
            }
          }
          
          i <- i + 1
          skiprows = skiprows + in_chunk_size + 
            # skips the header as well but only at the first chunk
            ifelse(i == 1 & header, 1, 0)
          rows <- tmpdt[,.N]
          if(rows < in_chunk_size) {
            done <- TRUE
          }
          
          # add to chunk
          add_chunk(outdf, tmpdt)
          rm(tmpdt); gc()
        }
        
        message(glue::glue("read {in_chunk_size*(i-1) + rows} rows from {infile}"))
        
        # remove the files
        fs::dir_delete(tmpdir1)
      }
    } else { # so shard by some element
      if(is.null(in_chunk_size)) {
        return(
          shard(inmapfn(data.table::fread(infile, header=header, ...)), shardby = shardby, nchunks = nchunks, outdir = outdir, overwrite = overwrite, compress = compress,...)
        )
      } else {
        i <- 0
        tmpdir1 = tempfile(pattern="df_tmp")
        fs::dir_create(tmpdir1)
        #message(tmpdir1)
        
        done = FALSE
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
                header=FALSE, ...))
            } else {
              tmpdt = inmapfn(data.table::fread(
                infile,
                skip = skiprows, nrows = in_chunk_size, 
                header=FALSE, col.names = column_names, ...))
            }
          }
          
          i <- i + 1
          skiprows = skiprows + in_chunk_size + 
            # skips the header as well but only at the first chunk
            ifelse(i == 1 & header, 1, 0)
          rows <- tmpdt[,.N]
          if(rows < in_chunk_size) {
            done <- TRUE
          }
          
          tmp.disk.frame = shard(
            tmpdt, 
            shardby = shardby, 
            nchunks = nchunks, 
            outdir = file.path(tmpdir1,i), 
            overwrite = TRUE,
            compress = compress,...)
          rm(tmpdt); gc()
        }
        
        message(glue::glue("read {in_chunk_size*(i-1) + rows} rows from {infile}"))
        #
        # do not run this in parallel as the level above this is likely in parallel
        # ZJ:
        system.time(
          fnl_out <- 
            rbindlist.disk.frame(
              lapply(
                list.files(
                  tmpdir1, full.names = TRUE), disk.frame), 
              outdir = outdir, by_chunk_id = TRUE, parallel=FALSE, overwrite = overwrite, compress = compress))
        
        # remove the files
        fs::dir_delete(tmpdir1)
      }
    }
    df = disk.frame(outdir)
    df = add_meta(df, nchunks=disk.frame::nchunks(df), shardkey = shardby, shardchunks = nchunks, compress = compress)
    return(df)
  }
}