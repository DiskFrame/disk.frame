#' Convert CSV file(s) to disk.frame format
#' 
#' Converts one or more CSV files to a disk frame by converting each file (or chunks of a file) to 
#' fst files. Those fst files are combined into a single disk frame.
#' 
#' @section Multiple files:
#' Multiple files can be read by providing a character vector of file names as the infile parameter.
#' If multiple files are provided, then the parameters nchunks, backend, chunk_reader,
#' and header_row can be either a single value or a vector of values, applied to each file respectively.
#' Note that at this time, backend_args will be applied to every file provided.
#' 
#' By default, each file will correspond to a single chunk in the resulting disk.frame. If the parameter
#' nchunks is set to greater than 1 for a given file, each chunk of that file will be a chunk in the 
#' resulting disk.frame. Thus, if three file names are provided, and nchunks = c(1,2,3), the resulting
#' disk.frame will contain 6 chunks. 
#' 
#' @section Backend:
#' Disk.frame does not have its own CSV reader. Instead, it uses one of several existing packages to read CSVs.
#'  
#' It is worth noting that data.table::fread does not detect dates and all dates 
#' are imported as strings, and you are encouraged to use {fasttime} to convert the strings to
#' date. You can use the \code{\link{inmapfn}} to do that. However, if you want automatic
#' date detection, then backend="readr" may suit your needs. However, `readr`
#' is often slower than data.table, hence data.table is chosen as the default.
#' `LaF` is another option, but is limited in its input parameters. In particular, you may need to 
#' set header=TRUE when relying on the `LaF` backend.
#' 
#' @section Chunking:
#' Even if you choose a backend there can still be multiple
#'   strategies on how to approach the CSV reads. For example, \code{\link[data.table]{fread}}
#'   tries to mmap the whole file which may cause the whole read process to
#'   fail. Recent updates to \code{\link[data.table]{fread}} seek to address this issue.
#'   
#'   \code{\link{estimate_chunks_needed}} can estimate the number of chunks required for a given csv file or files. 
#'   Keep in mind that by default, disk.frame will treat each csv file as a separate chunk for a single disk.frame
#'   if multiple csv files are provided as the infile parameter. Each chunk will be read into memory as needed
#'   when manipulating a disk frame. Therefore, you will want to keep enough memory overhead to permit the manipulation
#'   of each chunk in memory. (You may be able to input a large csv file in one chunk, 
#'   but that is useless if you do not have sufficient memory to do anything with it.)
#'   
#'   If chunks are needed, the easiest approach may be to use \code{\link{split_csv_file}} 
#'   to split the csv file prior to importing it with \code{\link{csv2disk.frame.}}. 
#'   
#'   Alternatively, you can select one of three chunk_readers: readLines, readr, or LaF.
#'   Each chunk_reader will read a portion of the csv file into memory and save that portion to an fst file, 
#'   before moving to the next chunk of the file. 
#'   
#'   The `readLines` chunk_reader uses \code{\link[base]{readLines}} to read each file chunk into memory.   
#'   The `readr` chunk_reader uses \code{\link[readr]{read_lines_chunked}}.
#'   The `LaF` chunk reader uses \code{\link[Laf]{process_blocks}}.
#'   
#'   Some combinations of chunk_reader and backend are not compatible or are not efficient.
#'   The following table specifies what is used for each combination:
#'   
#' \tabular{llll}{
#' \emph{chunk_reader}                  \tab \tab \emph{backend}                                                                  \tab                                                                                                    \cr
#'                 \tab \strong{data.table}                                                               \tab \strong{readr}                                                          \tab \strong{LaF}                                                                 \cr
#'    \strong{readr}        \tab \code{\link[readr]{read_lines_chunked}}; \code{\link[data.table]{fread}} \tab \code{\link[readr]{read_delim_chunked}}                        \tab \code{\link[readr]{read_lines_chunked}}; \code{\link[Laf]{detect_dm_csv}} \cr
#'    \strong{readLines}    \tab \code{\link[base]{readLines}}; \code{\link[data.table]{fread}}           \tab \code{\link[base]{readLines}}; \code{\link[readr]{read_delim}} \tab \code{\link[base]{readLines}}; \code{\link[Laf]{detect_dm_csv}} \cr
#'    \strong{LaF}          \tab Throws error.                                                            \tab Throws error.                                                  \tab \code{\link[Laf]{process_blocks}}; \code{\link[Laf]{detect_dm_csv}} \cr
#' }
#' 
#' @param infile The input CSV file or files
#' @param outdir The directory to output the disk.frame.
#' @param header_row Whether the files have header. Defaults to NULL, in which case \code{\link{header_row_index}}
#' will be used to guess the correct header index. Can be set to TRUE, FALSE, or a positive integer if the 
#' header row is not the first line of the file. May be an integer vector with a length equal to the length of infile.
#' @param backend The CSV reader backend to choose: "data.table", "readr", or "LaF," which will use
#' \code{\link[data.table]{fread}}, \code{\link[readr]{read_delim}}, or \code{\link[Laf]{detect_dm_csv}}, respectively.
#' @param backend_args List of arguments to pass to the backend function 
#' (\code{\link[data.table]{fread}}, \code{\link[readr]{read_delim}}, or \code{\link[Laf]{detect_dm_csv}}).
#' @param nchunks Number of chunks to output. May be an integer vector with a length equal to the length of infile.
#' If NULL (default), then the number of chunks will be estimated using \code{\link{estimate_chunks_needed}}.
#' @param chunk_reader The method to read each file chunk into memory: "readLines," "readr," or "LaF,"
#' which will use \code{\link[base]{readLines}}, \code{\link[readr]{read_lines_chunked}}, or 
#' \code{\link[Laf]{process_blocks}}, respectively.
#' @param max_percent_ram Maximum percentage of ram to use when estimating the number of chunks using \code{\link{estimate_chunks_needed}}.
#' @param inmapfn A function to be applied to the chunk read in from CSV before
#'   the chunk is being written out. Commonly used to perform simple
#'   transformations. Defaults to the identity function (ie. no transformation)
#' @param compress For fst backends it's a number between 0 and 100 where 100 is
#'   the highest compression ratio.
#' @param overwrite Whether to overwrite the existing directory
#' @param shardby The column(s) to shard the data by. For example suppose
#'   `shardby = c("col1","col2")`  then every row where the values `col1` and
#'   `col2` are the same will end up in the same chunk; this will allow merging
#'   by `col1` and `col2` to be more efficient.
#' @param .progress A logical, for whether or not to print a progress bar for
#'   multiprocess, multisession, and multicore plans. From {furrr}
#' @family ingesting data
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
csv_to_disk.frame <- function(infile, 
                              outdir = tempfile(fileext = ".df"), 
                              header_row = NULL,
                              backend = c("data.table", "readr", "LaF"),
                              backend_args = list(),
                              nchunks = NULL, # integer or vector equal to length of infiles
                              chunk_reader = c("readr", "readLines", "LaF"),
                              max_percent_ram = 0.5,
                              inmapfn = base::I, 
                              compress = 50,
                              overwrite = TRUE,
                              shardby = NULL,
                              .progress = TRUE) {
  # backend_args need be used instead of ... to avoid situations where
  # the ... include parameters with names that begin the same as arguments to csv_to_disk.frame
  # for example, if ... has the parameter "header", that value will be assigned to header_row erroneously.
  # Given the number of potential options for ... parameters, using a specified list is a safer choice.
  
  ##### Initial checks #####
  backend = match.arg(backend)
  chunk_reader = match.arg(chunk_reader)

  if(!is.null(shardby)) warning("Sharding is currently not implemented for csv_to_disk.frame.")
  
  if(backend == "readr" | chunk_reader == "readr") {
    if(!requireNamespace("readr")) {
      stop("csv_to_disk.frame: You have chosen backend = 'readr' or chunk_reader = 'readr'. But `readr` package is not installed. To install run: `install_packages(\"readr\")`")
    }
  }
  
  if(backend == "LaF" | chunk_reader == "LaF") {
    if(!requireNamespace("LaF")) {
      stop("csv_to_disk.frame: You have chosen backend = 'LaF' or chunk_reader = 'LaF'. But `LaF` package is not installed. To install run: `install_packages(\"LaF\")`")
    }
  }
  
  # if(backend == "LaF" & chunk_reader != backend) {
  #   # The LaF chunking function is unique to LaF, so the two should be used together
  #   message("When LaF is selected as the the chunk_reader, it will be used for backend as well.")
  #   chunk_reader <- backend <- "LaF"
  # }
  
  if(chunk_reader == "LaF" & backend != "LaF") {
    # need to throw error because user may need to pass in different dots arguments for LaF backend.
    # LaF chunk reader only uses the LaF backend
    stop("The LaF chunk_reader is only supported when the LaF backend is selected.")
  }
  
  stopifnot(is.null(nchunks) | length(nchunks) == 1 | length(nchunks) == length(infile))
  stopifnot(file.exists(infile))
  overwrite_check(outdir, overwrite)
  if(!dir.exists(outdir)) dir.create(outdir)
  
  ##### Detect column classes #####
  # do on all csv files simultaneously to avoid class conflicts
  # data.table::fread uses colClasses, header ("auto", TRUE, FALSE), skip 
  # readr::read_delim uses col_names (TRUE, FALSE, or character vector), col_types, skip
  # readr::read_delim_chunked uses same as readr::read_delim
  # LaF::detect_dm_csv uses header (TRUE, FALSE), possibly skip, col.names, column_types, from read.table
  # Get column types, if necessary
  if(backend == "data.table" & !("colClasses" %in% names(backend_args))) {
    backend_args$colClasses <- do.call(determine_classes_csv_data.table, c(list(infile), backend_args))
  } else if(backend == "readr" & !("col_types" %in% names(backend_args))) {
    backend_args$col_types <- do.call(determine_classes_csv_readr, c(list(infile), backend_args))
  } else if(backend == "LaF" & !("column_types" %in% names(backend_args))) {
    backend_args$colClasses <- do.call(determine_classes_csv_laf, c(list(infile), backend_args))
  }
  
  ##### Chunk size #####
  # set chunk size based on estimated file size if not yet set
  if(is.null(nchunks)) {
    nchunks <- estimate_chunks_needed(infile = infile, max_percent_ram = max_percent_ram)
  } else if(length(nchunks) == 1) {
    nchunks <- rep(nchunks, times = length(infile))
  }
  
  ##### Header row index #####
  if(is.null(header_row)) {
    header_row <- mapply(header_row_index, csv_file = infile, method = backend, MoreArgs = backend_args)
  }
  
  ##### CSV import #####
  # convert csvs to fst files contained in the outdir
  fst_files <- csv2fst(infile = infile,
                       nchunks = nchunks,
                       backend = backend,
                       chunk_reader = chunk_reader,
                       header_row = header_row,
                       outdir = outdir,
                       inmapfn = inmapfn,
                       compress = compress,
                       .progress = .progress,
                       backend_args = backend_args)
  fst_files <- unlist(fst_files)
  stopifnot(all(dirname(fst_files) == outdir))
  
  dff <- disk.frame(outdir)
  
  ##### Shard if necessary #####
  # Currently fails: 
  # for shardby_function = "hash", Error: `path` must be a directory, in rechunk function
  # for shardby_function = "sort",  Error in rechunk(df, shardby = shardby, nchunks = nchunks_rechunk, outdir = outdir,  nchunks must be larger than 1 
  # if(!is.null(shardby)) {
  #   dff <- shard(dff,
  #                shardby = shardby,
  #                outdir = outdir,
  #                overwrite = TRUE,
  #                nchunks = nchunks(dff))
  # }
  if(!is.null(shardby)) warning("shardby currently not functional; will be ignored.")
  
  return(dff) 
}




#' Convert a csv file to an fst file
#' 
#' @param infile One or more csv files
#' @param nchunks Number of chunks to use when reading the csv file(s)
#' @param backend CSV file reader to use. 
#' @param chunk_reader Method to use to read chunks. 
#' @param inmapfn A function to be applied to the chunk read in from CSV before 
#' the chunk is being written out.
#' @param outdir Directory to save the fst files.
#' @param compress Compression level for fst files. See \code{\link{fst::write_fst}}
#' @param .progress If TRUE, display progress when importing csv files. 
#' @param ... Additional parameters for backend data.table, readr, or LaF. 
#' @seealso \code{\link{csv_to_disk.frame}}
#'          \code{\link[readr]{read_delim_chunked}}
#' 
#' @importFrom fs file_move
#' @keywords internal
csv2fst <- function(infile, 
                    nchunks, 
                    backend = c("data.table", "readr", "LaF"), 
                    chunk_reader = c("readr", "readLines", "LaF"), 
                    header_row = 1,
                    inmapfn = base::I,
                    outdir = tempfile(fileext = ".df"),
                    compress = 50,
                    .progress = TRUE,
                    backend_args = backend_args) {
  if(length(infile) > 1) return(mapply(csv2fst, 
                                       infile = infile, 
                                       nchunks = nchunks, 
                                       chunk_reader = chunk_reader,
                                       backend = backend,
                                       header_row = header_row,
                                       MoreArgs = c(list(inmapfn = inmapfn,
                                                         outdir = outdir,
                                                         compress = compress,
                                                         .progress = .progress,
                                                         backend_args = backend_args)),
                                       SIMPLIFY = FALSE))
  backend <- match.arg(backend)
  chunk_reader <- match.arg(chunk_reader)
  
  stopifnot(length(nchunks) == length(infile))
  
  if(backend == "readr" & !("delim" %in% names(backend_args))) {
    # readr::read_delim_chunked does not provide a default delim parameter
    backend_args$delim <- ","
  }
  
  import_fn <- switch(backend,
                      data.table = csv2fst_data.table,
                      readr = csv2fst_readr,
                      LaF = csv2fst_laf)
  
  
  # chunk_reader == data.table or bigreadr --> handled prior
  if(chunk_reader == "readLines" & nchunks > 1) {
    # create function that will sequentially return blocks of the file, with headers
    chunk_reader_fn <- chunk_reader_fn(infile,
                                       nchunks = nchunks,
                                       num_header_rows = header_row)
    fst_file_vec <- NULL
    while(!is.null(xx <- chunk_reader_fn())) {
      # save xx to fst using data.table, readr, or LaF
      fst_file <- do.call(import_fn, c(list(xx,
                                            inmapfn = inmapfn, 
                                            compress = compress, 
                                            outdir = outdir),
                                       backend_args))
      fst_file_vec <- c(fst_file_vec, fst_file)
    }
    
  } else if(chunk_reader == "LaF" & nchunks > 1) {
    fst_file_vec <- do.call(csv2fst_laf, c(list(csv_file = infile, 
                                                nchunks = nchunks,
                                                inmapfn = inmapfn,
                                                outdir = outdir,
                                                compress = compress),
                                           backend_args))
  } else if(chunk_reader == "readr" & nchunks > 1) {
    stopifnot(requireNamespace("readr"))
    n <- count_lines_in_file(infile)
    in_chunk_size <- ceiling(n / nchunks)
    
    if(backend == "readr") {
      # use read_delim_chunked
      callback_fn <- function(x, pos) {
        df2fst(x, 
               inmapfn = inmapfn, 
               compress = compress, 
               outdir = outdir)
      }
      
      fst_file_vec <- do.call(readr::read_delim_chunked, c(list(file = infile,
                                                                callback = readr::ListCallback$new(callback_fn),
                                                                chunk_size = in_chunk_size,
                                                                progress = .progress),
                                                           backend_args))
    } else {
      callback_fn <- do.call(chunk_reader_fn_readr, c(list(reader_fn = import_fn,
                                                           num_header_rows = header_row,
                                                           outdir = outdir,
                                                           inmapfn = inmapfn,
                                                           compress = compress),
                                                      backend_args))
      fst_file_vec <- readr::read_lines_chunked(file = infile,
                                                callback = readr::ListCallback$new(callback_fn),
                                                chunk_size = in_chunk_size,
                                                progress = .progress)
    }
  } else {
    fst_file_vec <- do.call(import_fn, c(list(infile, 
                                              inmapfn = inmapfn, 
                                              compress = compress, 
                                              outdir = outdir),
                                         backend_args))
  }
  
  fst_file_vec <- unlist(fst_file_vec)
  
  # rename files for convenience
  if(length(fst_file_vec) > 1) {
    new_path <- renumber_file_names(files = file.path(dirname(fst_file_vec), basename(infile)), 
                              suffix = "chunk", 
                              extension = "fst") 
  } else {
    new_path <- file.path(dirname(fst_file_vec), fs::path_ext_set(basename(infile), ext = "fst"))
  }
  
  out <- fs::file_move(path = fst_file_vec, 
                       new_path = new_path)
  stopifnot(file.exists(out))
  
  return(out)
}

#' Convert csv file to fst file using data.table::fread.
#' @importFrom data.table fread
#' @keywords internal
csv2fst_data.table <- function(csv_file, 
                               inmapfn = base::I, 
                               compress = 50, 
                               outdir = tempfile(fileext = ".df"),
                               ...) {
  dots <- list(...)

  if(length(csv_file) > 1 | any(grepl("\n|\r", csv_file))) {
    dots$text <- csv_file
  } else {
    dots$input = csv_file
  }
  chunk <- do.call(data.table::fread, dots)
  fst_file <- df2fst(chunk, 
                     inmapfn = inmapfn, 
                     compress = compress, 
                     outdir = outdir)
  return(fst_file)
}

#' convert csv file to fst file using readr::read_delim
#' @seealso \code{\link[readr]{read_delim}}, 
#'          \code{\link[readr]{read_delim_chunked}}
#' @keywords internal
csv2fst_readr <- function(csv_file,
                          nchunks = 1,
                          inmapfn = base::I, 
                          compress = 50, 
                          outdir = tempfile(fileext = ".df"),
                          delim=",",
                          ...) {
  stopifnot(requireNamespace("readr"))
  if(nchunks == 1) {
    dat <- readr::read_delim(file = csv_file, delim = delim, ...)
    fst_file <- df2fst(dat, 
                       inmapfn = inmapfn, 
                       compress = compress, 
                       outdir = outdir)
    return(fst_file)
  } 
  
  n <- count_lines_in_file(csv_file)
  in_chunk_size <- ceiling(n / nchunks)
  
  callback_fn <- function(x, pos) {
    df2fst(x, 
           inmapfn = inmapfn, 
           compress = compress, 
           outdir = outdir)
  }
  
  fst_files.lst <- readr::read_delim_chunked(file = csv_file, 
                                      callback = readr::ListCallback$new(callback_fn),
                                      chunk_size = in_chunk_size, 
                                      delim = delim, 
                                      ...)
  
  return(unlist(fst_files.lst))
}


#' convert csv to fst file using LaF::detect_dm_csv
#' re-save to file if a character vector is passed instead of a file name.
#' @seealso \code{\link[LaF]{detect_dm_csv}}, 
#'          \code{\link[LaF]{laf_open}}, 
#'          \code{\link[LaF]{next_block}}
#' @keywords internal
csv2fst_laf <- function(csv_file, 
                        nchunks = 1,
                        inmapfn = base::I,
                        outdir = tempfile(fileext = ".df"),
                        compress = 50,
                        ...) {
  stopifnot(requireNamespace("LaF"))
  # check for character vector and re-save if necessary
  if(length(csv_file) > 1 | any(grepl("\n", csv_file))) {
    n <- length(csv_file)
    chunk_tmp_file <- tempfile(fileext = ".csv")
    writeLines(text = csv_file, con = chunk_tmp_file) 
    csv_file <- chunk_tmp_file
  } else {
    n <- count_lines_in_file(csv_file)
  }
  
  dm <- LaF::detect_dm_csv(filename = csv_file, ...)
  model <- LaF::laf_open(dm)
  fst_file_vec <- NULL
  in_chunk_size <- ceiling(n / nchunks)
  while(nrow(chunk <- LaF::next_block(model, nrows = in_chunk_size)) > 0) {
    fst_file <- df2fst(chunk, 
                       inmapfn = inmapfn, 
                       compress = compress, 
                       outdir = outdir)
    fst_file_vec <- c(fst_file_vec, fst_file)
  }
  
  return(fst_file_vec)
}


#' creates callback function that can be used with readr::read_lines_chunked
#' @keywords internal
chunk_reader_fn_readr <- function(reader_fn,
                                  num_header_rows = 0,
                                  ...) {
  chunk_reader_args <- list(...)
  header <- NULL
  
  function(x, pos) {
    if(pos == 1 & num_header_rows > 0) {
      header <<- x[1:num_header_rows]
    } else if(num_header_rows > 0) {
      x <- c(header, x)
    }
    args <- c(list(x), chunk_reader_args)
    do.call(reader_fn, args)
  }
}

#' creates function that will read a file in successive parts using readLines.
#' @keywords internal
chunk_reader_fn <- function(infile, 
                            nchunks = 1,
                            num_header_rows = 0) {
  force(infile)
  force(nchunks)
  force(num_header_rows)
  
  chunk_number <- 1
  con <- file(infile, "r")
  n <- count_lines_in_file(infile)
  in_chunk_size <- ceiling(((n - num_header_rows) / nchunks)) 
  
  header <- NULL
  if(num_header_rows > 0) {
    header <- readLines(con, n = num_header_rows)
  }
  
  function(force_close = FALSE) {
    if(force_close) {
      try(close(con))
      return(NULL)
    }
    txt <- readLines(con, n = in_chunk_size)
    if(length(txt) == 0) {
      try(close(con))
      return(NULL)
    }
    
    chunk_number <<- chunk_number + 1
    return(c(header, txt))
  }
}


#' Saves a dataframe to an fst file.
#' @importFrom fst write_fst
#' @importFrom fs path_ext_set
#' @keywords internal
df2fst <- function(df, 
                   inmapfn = base::I, 
                   compress = 50,
                   outdir = tempdir(),
                   outfile_template = tempfile()) {
  if(!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
  
  fst_file_path <- file.path(outdir, fs::path_ext_set(basename(outfile_template), ".fst"))
  df %>%
    inmapfn() %>%
    fst::write_fst(path = fst_file_path, compress = compress)
  
  stopifnot(file.exists(fst_file_path))
  
  return(fst_file_path)
}



#' Estimate number of chunks needed
#' 
#' Estimate number of chunks needed for one or more files. Does this by assuming the file size is 
#' commensurate with amount of RAM required, and comparing to total RAM available.
#' Can be offset by setting max_percent_ram. In general, set max_percent_ram to well under 1.0 if
#' the files may be read in parallel or if you anticipate copies may be made. 
#' @param infile One or more file locations, as character vector.
#' @param max_percent_ram Maximum percent RAM that should be devoted to the file import.
#' @param ... Currently unused
#' @return Integer vector, length of infile, indicating the number of chunks that should be used.
#' 
#' @importFrom fs file_size
#' @keywords internal
estimate_chunks_needed <- function(infile, max_percent_ram = 0.5, ...) {
  if(max_percent_ram >= 1) warning(sprintf("Maximum percent ram to use to load CSV files is %.02f.\nThis value should probably be less than 1.", max_percent_ram))
  file_sizes <- fs::file_size(infile) # bytes
  sys_ram <- df_ram_size() # in GB
  
  idx <- (file_sizes/(1024^3)) > (sys_ram * max_percent_ram)
  
  nchunks <- rep(1, times = length(infile))
  if(any(idx)) {
    files_to_split <- infile[idx]
    message(sprintf("Will chunk %d files for import due to memory constraints:\n\t%s", 
                    length(files_to_split),
                    paste(basename(files_to_split), collapse = "\n\t")))
    
    # determine minimum chunks for each file
    # evenly chunk csv so that a given chunk is less than the memory limitation
    files_to_split_size <- file_sizes[idx]/(1024^3)
    # files_to_split_size <- c(30, 40, 50, 26, 100)
    split_chunks <- ceiling(files_to_split_size / (sys_ram * max_percent_ram))
    nchunks[idx] <- split_chunks
  }
  return(nchunks)
}

#' Determine classes for one or more files using data.table::fread by reading in 
#' first row. In case of conflict between files, use the more general class.
#' (follows data.table approach)
#' @keywords internal
determine_classes_csv_data.table <- function(infiles, ...) {
  dots <- list(...)
  dots$input <- NULL
  dots$text <- NULL
  dots$cmd <- NULL
  dots$nrows <- 1
  head.lst <- lapply(infiles, function(f, dots) {
    dots$file <- f
    do.call(data.table::fread, dots)
  }, dots = dots)
  
  stopifnot(all(ncol(head.lst[[1]]) == sapply(head.lst, ncol)))
  res <- dplyr::bind_rows(lapply(head.lst, function(df) { dplyr::bind_rows(lapply(df, class)) } ))
  
  # select the maximum level if more than one type found for a column
  lvls <- c("logical", "Date", "integer", "numeric", "character")
  stopifnot(unique(sapply(res, unique) %>% unlist) %in% lvls)
  res %>%
    dplyr::mutate_all(~ factor(., levels = lvls, ordered = TRUE)) %>%
    dplyr::summarize_all(max) %>%
    dplyr::mutate_all(~ as.character(.)) %>%
    unlist 
}

#' Read first row of a file or files using readr::read_delim and use the result to 
#' determine classes. In case of conflict between files, use the more general class.
#' @keywords internal
determine_classes_csv_readr <- function(infiles, delim=",", ...) {
  dots <- list(...)
  dots$n_max <- 1
  dots$delim <- delim
  dots$progress <- FALSE
  head.lst <- lapply(infiles, function(f, dots) {
    dots$file <- f
    suppressMessages(do.call(readr::read_delim, dots))
  }, dots = dots)
  
  stopifnot(all(ncol(head.lst[[1]]) == sapply(head.lst, ncol)))
  res <- bind_rows(lapply(head.lst, function(df) { bind_rows(lapply(df, . %>% class %>% first)) } ))
  
  # select the maximum level if more than one type found for a column
  lvls <- c("logical", "Date", "POSIXct", "integer", "numeric", "character")
  stopifnot(unique(sapply(res, unique) %>% unlist) %in% lvls)
  
  col_types <- res %>%
    dplyr::mutate_all(~ factor(., levels = lvls, ordered = TRUE)) %>%
    dplyr::summarize_all(max) %>%
    dplyr::mutate_all(~ as.character(.)) %>%
    unlist 
  
  plyr::revalue(col_types, 
                replace = c(logical = "l",
                            Date = "D",
                            POSIXct = "T",
                            integer = "i",
                            numeric = "n",
                            character = "c",
                            "?"),
                warn_missing = FALSE)
}

#' Read a file or files using LaF::detect_dm_csv and use the created model to 
#' get the column classes. In case of conflict between files, use the more general class.
#' @keywords internal
determine_classes_csv_laf <- function(infiles, ...) {
  model.lst <- lapply(infiles, LaF::detect_dm_csv, ...)
  
  res <- sapply(model.lst, function(model) {
    out <- model$columns$type
    names(out) <- model$columns$name
    out })
  res <- as.data.frame(t(res))
  
  # valid types are double, integer, categorical, string
  # see LaF::laf_open_csv
  # but LaF actually relies on read.table, so the parameter is colClasses and 
  # types are character, integer, numeric, factor
  # read.table will also accept Date, POSIXct, raw, logical, complex -- but LaF does not accept those 
  # (I have not tested every option, so some might be accepted.)
  # select the maximum level if more than one type found for a column
  lvls <- c("integer", "double", "categorical", "string")
  stopifnot(unique(sapply(res, unique) %>% unlist) %in% lvls)
  
  new_lvls <- c("integer", "numeric", "factor", "character")
  
  
  res %>%
    dplyr::mutate_all(as.character) %>%
    dplyr::mutate_all(~ case_when(. == "double" ~ "numeric",
                                  . == "categorical" ~ "factor",
                                  . == "string" ~ "character",
                                  TRUE ~ .)) %>%
    dplyr::mutate_all(~ factor(., levels = new_lvls, ordered = TRUE)) %>%
    dplyr::summarize_all(max) %>%
    dplyr::mutate_all(as.character) %>%
    unlist 
}
