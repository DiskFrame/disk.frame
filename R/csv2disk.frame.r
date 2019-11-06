#' Convert CSV file(s) to disk.frame format
#' @importFrom glue glue
#' @importFrom fs dir_delete
#' @importFrom pryr do_call
#' @param infile The input CSV file or files
#' @param outdir The directory to output the disk.frame to
#' @param inmapfn A function to be applied to the chunk read in from CSV before
#'   the chunk is being written out. Commonly used to perform simple
#'   transformations. Defaults to the identity function (ie. no transformation)
#' @param nchunks Number of chunks to output
#' @param in_chunk_size When reading in the file, how many lines to read in at
#'   once. This is different to nchunks which controls how many chunks are
#'   output
#' @param shardby The column(s) to shard the data by. For example suppose
#'   `shardby = c("col1","col2")`  then every row where the values `col1` and
#'   `col2` are the same will end up in the same chunk; this will allow merging
#'   by `col1` and `col2` to be more efficient
#' @param compress For fst backends it's a number between 0 and 100 where 100 is
#'   the highest compression ratio.
#' @param overwrite Whether to overwrite the existing directory
#' @param header Whether the files have header. Defaults to TRUE
#' @param .progress A logical, for whether or not to print a progress bar for
#'   multiprocess, multisession, and multicore plans. From {furrr}
#' @param backend The CSV reader backend to choose: "data.table" or "readr". 
#'   disk.frame does not have its own CSV reader. It uses either
#'   data.table::fread or readr::read_delim. It is worth noting that
#'   data.table::fread does not detect dates and all dates are imported as
#'   strings, and you are encouraged to use {fasttime} to convert the strings to
#'   date. You can use the `inmapfn` to do that. However, if you want automatic
#'   date detection, then backend="readr" may suit your needs. However, readr
#'   is often slower than data.table, hence data.table is chosen as the default.
#' @param chunk_reader Even if you choose a backend there can still be multiple
#'   strategies on how to approach the CSV reads. For example, data.table::fread
#'   tries to mmap the whole file which can cause the whole read process to
#'   fail. In that case we can change the chunk_reader to "readLines" which uses the
#'   readLines function to read chunk by chunk and still use data.table::fread
#'   to process the chunks. There are currently no strategies for readr backend,
#'   except the default one.
#' @param ... passed to data.table::fread, disk.frame::as.disk.frame,
#'   disk.frame::shard
#' @importFrom pryr do_call
#@importFrom LaF detect_dm_csv process_blocks
# @importFrom bigreadr split_file get_split_files
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
                              inmapfn = base::I, 
                              in_chunks = NULL, # integer or vector equal to length of infiles
                              import_ram_percentage = 0.8,
                              shardby = NULL,
                              compress = 50,
                              overwrite = TRUE,
                              .progress = TRUE,
                              backend = c("data.table", "readr", "LaF"), 
                              chunk_reader = c("bigreadr", "data.table", "readr", "readLines", "LaF"),
                              backend_args = list(),
                              chunk_reader_args = list()) {
  
  ##### Initial checks #####
  backend = match.arg(backend)
  chunk_reader = match.arg(chunk_reader)
  
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
  
  if(chunk_reader == "bigreadr") {
    if(!requireNamespace("bigreadr")) {
      stop("csv_to_disk.frame: You have chosen chunk_reader = 'bigreadr'. But `bigreadr` package is not installed. To install run: `install_packages(\"bigreadr\")`")
    }
  }
  
  # simplify choices where it probably makes no sense to go with other chunking options
  if(backend == "readr" & chunk_reader %in% c("LaF", "readLines")) {
    # readr has a dedicated chunking function, so should just use it
    # maybe worth using a file-splitting chunk_reader first
    message("When readr is selected as the backend, it will be used for chunk_reader in lieu of 'Laf' or 'readLines' as well.")
    chunk_reader <- backend <- "readr"
  }
  
  if(backend == "LaF" & chunk_reader != backend) {
    # The LaF chunking function is unique to LaF, so the two should be used together
    message("When LaF is selected as the the chunk_reader, it will be used for backend as well.")
    chunk_reader <- backend <- "LaF"
  }
  
  if(chunk_reader == "LaF" & backend != "LaF") {
    # need to throw error because user may need to pass in different dots arguments for LaF backend.
    # LaF chunk reader only uses the LaF backend
    stop("The LaF chunk_reader is only supported when the LaF backend is selected.")
  }
  
  stopifnot(is.null(in_chunks) | length(in_chunks) == 1 | length(in_chunks) == length(infile))
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
  
  # for chunk readers, data.table and bigreadr take arguments, including classes
  # readr, readLines, and LaF rely on backend_args
  
  
  
  if(chunk_reader == "data.table" & !("colClasses" %in% names(chunk_reader))) {
    chunk_reader_args$colClasses <- do.call(determine_classes_csv_data.table, c(list(infile), chunk_reader_args))
  } 
  
  if(chunk_reader %in% c("readr", "readLines", "LaF") & length(chunk_reader_args) > 0) {
    message("For chunk_reader 'readr,' 'readLines,' and 'LaF', the chunk_reader_args parameter is not needed.")
  }
  
  
  ##### Chunk size #####
  # set chunk size based on estimated file size if not yet set
  if(is.null(in_chunks)) {
    in_chunks <- estimate_chunk_size(infiles = infile, max_percent_ram = import_ram_percentage)
  } else if(length(in_chunks) == 1) {
    in_chunks <- rep(in_chunks, times = length(infile))
  }
  
  
  ##### Split files #####
  # bigreadr: splits files
  # data.table: split files
  idx <- in_chunks > 1 & chunk_reader %in% c("bigreadr", "data.table")
  if(any(idx)) {
    infile[idx] <- mapply(split_csv_file, 
                          infile = infile[idx], 
                          in_chunks = in_chunks[idx],
                          MoreArgs = c(chunk_reader_args, list(chunk_reader = chunk_reader)),
                          SIMPLIFY = FALSE)
    infile <- infile %>% unlist
    in_chunks <- rep(1, times = length(infile))
  }
  
  
  ##### CSV import #####
  # convert csvs to fst files contained in the outdir
  fst_files <- csv2fst(infile = infile,
                       in_chunks = in_chunks,
                       backend = backend,
                       chunk_reader = chunk_reader,
                       outdir = outdir,
                       inmapfn = inmapfn,
                       compress = compress,
                       .progress = .progress,
                       backend_args = backend_args)
  fst_files <- unlist(fst_files)
  
  
  stopifnot(all(dirname(fst_files) == outdir))
  
  dff <- disk.frame(outdir)
  
  ##### Shard if necessary #####
  # currently not working. 
  # Possibly because disk.frame is not saving a .metadata file,
  # and shard is expecting one? Throwing error  
  # Error: `path` must be a directory 
  # fs::dir_copy(file.path(outdir, ".metadata"), file.path(back_up_tmp_dir, 
  # ".metadata")) at rechunk.r#45
  
  # if(!is.null(shardby)) {
  #   dff <- shard(dff, 
  #                shardby = shardby, 
  #                outdir = outdir, 
  #                overwrite = TRUE, 
  #                nchunks = nchunks(dff))
  # }
  
  return(dff) 
}

#' Convert a csv file to an fst file
#' 
#' @param infile One or more csv files
#' @param in_chunks Number of chunks to use when reading the csv file(s)
#' @param backend CSV file reader to use. 
#' @param chunk_reader Method to use to read chunks. 
#' Note that data.table and bigreadr are only there for compatibility; these options do nothing here
#' at a chunk level. Instead, they can be used by \code{\link{csv_to_disk.frame}} to split a large csv file
#' into multiple files. 
#' @param inmapfn A function to be applied to the chunk read in from CSV before 
#' the chunk is being written out.
#' @param outdir Directory to save the fst files.
#' @param compress Compression level for fst files. See \code{\link{fst::write_fst}}
#' @param .progress If TRUE, display progress when importing csv files. 
#' @param ... Additional parameters for backend data.table, readr, or LaF. 
#' @seealso \code{\link{csv_to_disk.frame}}
csv2fst <- function(infile, 
                    in_chunks, 
                    backend = c("data.table", "readr", "LaF"), 
                    chunk_reader = c("bigreadr", "data.table", "readr", "readLines", "LaF"), 
                    inmapfn = base::I,
                    outdir = tempfile(fileext = ".df"),
                    compress = 50,
                    .progress = TRUE,
                    backend_args = backend_args) {
  if(length(infile) > 1) return(mapply(csv2fst, 
                                       infile = infile, 
                                       in_chunks = in_chunks, 
                                       chunk_reader = chunk_reader,
                                       backend = backend,
                                       MoreArgs = c(list(inmapfn = inmapfn,
                                                         outdir = outdir,
                                                         compress = compress,
                                                         .progress = .progress,
                                                         backend_args = backend_args)),
                                       SIMPLIFY = FALSE))
  backend <- match.arg(backend)
  chunk_reader <- match.arg(chunk_reader)
  
  stopifnot(length(in_chunks) == length(infile))
  
  import_fn <- switch(backend,
                      data.table = csv2fst_data.table,
                      readr = csv2fst_readr,
                      LaF = csv2fst_laf)
  
  
  # chunk_reader == data.table or bigreadr --> handled prior
  if(chunk_reader == "readLines") {
    header_row_fn <- switch(backend,
                            data.table = header_row_index_data.table,
                            readr = header_row_index_readr,
                            LaF = header_row_index_laf)
    num_header_rows <- do.call(header_row_fn, c(list(infile), backend_args))
    
    # create function that will sequentially return blocks of the file, with headers
    chunk_reader_fn <- chunk_reader_fn(infile,
                                       in_chunks = in_chunks,
                                       num_header_rows = num_header_rows)
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
    
  } else if(chunk_reader == "LaF") {
    stopifnot(backend == "LaF")
    num_header_rows <- do.call(header_row_index_laf, c(list(infile), backend_args))
    
    fst_file_vec <- do.call(csv2fst_laf, c(list(csv_file = infile, 
                                                num_header_rows = num_header_rows, 
                                                in_chunks = in_chunks,
                                                inmapfn = inmapfn,
                                                outdir = outdir,
                                                compress = compress),
                                           backend_args))
  } else if(chunk_reader == "readr") {
    n <- count_lines_in_file(infile)
    in_chunk_size <- ceiling(n / in_chunks)
    
    if(backend == "readr") {
      # use read_delim_chunked
      callback_fn <- function(x, pos) {
        df2fst(x, 
               inmapfn = inmapfn, 
               compress = compress, 
               outdir = outdir)
      }
      
      if(!("delim" %in% names(backend_args))) {
        # readr::read_delim_chunked does not provide a default delim parameter
        backend_args$delim <- ","
      }
    
      fst_file_vec <- do.call(readr::read_delim_chunked, c(list(file = infile,
                                                                callback = readr::ListCallback$new(callback_fn),
                                                                chunk_size = in_chunk_size,
                                                                progress = .progress),
                                                           backend_args))
    } else {
      header_row_fn <- switch(backend,
                              data.table = header_row_index_data.table,
                              readr = header_row_index_readr,
                              LaF = header_row_index_laf)
      num_header_rows <- do.call(header_row_fn, c(list(infile), backend_args))
      
      callback_fn <- do.call(chunk_reader_fn_readr, c(list(reader_fn = import_fn,
                                                           num_header_rows = num_header_rows,
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

csv2fst_readr <- function(csv_file,
                          in_chunks = 1,
                          inmapfn = base::I, 
                          compress = 50, 
                          outdir = tempfile(fileext = ".df"),
                          delim=",",
                          ...) {
  if(in_chunks == 1) {
    dat <- readr::read_delim(file = csv_file, delim = delim, ...)
    fst_file <- df2fst(dat, 
                       inmapfn = inmapfn, 
                       compress = compress, 
                       outdir = outdir)
    return(fst_file)
  } 
  
  n <- count_lines_in_file(csv_file)
  in_chunk_size <- ceiling(n / in_chunks)
  
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



csv2fst_laf <- function(csv_file, 
                        num_header_rows, 
                        in_chunks = 1,
                        inmapfn = base::I,
                        outdir = tempfile(fileext = ".df"),
                        compress = 50,
                        ...) {
  n <- count_lines_in_file(csv_file)
  in_chunk_size <- ceiling(n / in_chunks)
  
  # check for character vector and re-save if necessary
  if(length(csv_file) > 1 | any(grepl("\n", csv_file))) {
    chunk_tmp_file <- tempfile(fileext = ".csv")
    write.csv(x = csv_file, file = chunk_tmp_file, ...) # may need to drop some arguments if not compatible
    csv_file <- chunk_tmp_file
  }
  
  dm <- LaF::detect_dm_csv(filename = csv_file, ...)
  model <- LaF::laf_open(dm)
  fst_file_vec <- NULL
  while(nrow(chunk <- LaF::next_block(model, nrows = in_chunk_size)) > 0) {
    fst_file <- df2fst(chunk, 
                       inmapfn = inmapfn, 
                       compress = compress, 
                       outdir = outdir)
    fst_file_vec <- c(fst_file_vec, fst_file)
  }
  
  return(fst_file_vec)
}



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


chunk_reader_fn <- function(infile, 
                            in_chunks = 1,
                            num_header_rows = 0) {
  force(infile)
  force(in_chunks)
  force(num_header_rows)
  
  chunk_number <- 1
  con <- file(infile, "r")
  n <- count_lines_in_file(infile)
  in_chunk_size <- ceiling(((n - num_header_rows) / in_chunks)) 
  
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


count_lines_in_file <- function(file) {
  num_lines <- tryCatch({ system2(command = "wc",
                                  args = c("--lines",
                                           file,
                                           "|",
                                           "awk -F  ' ' '{print $1}'"),
                                  stdout = TRUE) %>% as.integer() },
                        error = function(e) NA,
                        warning = function(e) NA)
  
  if(is.na(num_lines)) {
    if(requireNamespace("bigreadr")) {
      num_lines <- bigreadr::nlines(file)
    } else if(requireNamespace("LaF")) {
      num_lines <- determine_nlines(file)
    } else if(requireNamespace("R.utils")) {
      num_lines <- R.utils::countLines(file)
    } else {
      num_lines <- length(readLines(file))
    }
  }
  stopifnot(num_lines > 0)
  return(num_lines)
}


estimate_chunk_size <- function(infiles, max_percent_ram = 0.8, ...) {
  if(max_percent_ram >= 1) warning(sprintf("Maximum percent ram to use to load CSV files is %.02f.\nThis value should probably be less than 1.", max_percent_ram))
  file_sizes <- fs::file_size(infiles) # bytes
  sys_ram <- df_ram_size() # in GB
  
  idx <- (file_sizes/(1024^3)) > (sys_ram * max_percent_ram)
  
  in_chunks <- rep(1, times = length(infiles))
  if(any(idx)) {
    files_to_split <- infiles[idx]
    message(sprintf("Will chunk %d files for import due to memory constraints:\n\t%s", 
                    length(files_to_split),
                    paste(basename(files_to_split), collapse = "\n\t")))
    
    # determine minimum chunks for each file
    # evenly chunk csv so that a given chunk is less than the memory limitation
    files_to_split_size <- file_sizes[idx]/(1024^3)
    # files_to_split_size <- c(30, 40, 50, 26, 100)
    split_chunks <- ceiling(files_to_split_size / (sys_ram * max_percent_ram))
    in_chunks[idx] <- split_chunks
  }
  return(in_chunks)
}


# SplitLargeCSVFiles <- function(infiles, max_percent_ram = 0.8) {
#   if(max_percent_ram >= 1) warning(sprintf("Maximum percent ram to use to load CSV files is %.02f.\nThis value should probably be less than 1.", max_percent_ram))
#   
#   file_sizes <- fs::file_size(infiles) # bytes
#   sys_ram <- df_ram_size() # in GB
#   
#   idx <- (file_sizes/(1024^3)) > (sys_ram * max_percent_ram)
#   
#   if(any(idx)) {
#     files_to_split <- infiles[idx]
#     message(sprintf("Splitting %d files for import due to memory constraints:\n\t%s", 
#                     length(files_to_split),
#                     paste(basename(files_to_split), collapse = "\n\t")))
#     
#     # determine minimum chunks for each file
#     # evenly chunk csv so that a given chunk is less than the memory limitation
#     files_to_split_size <- file_sizes[idx]/(1024^3)
#     # files_to_split_size <- c(30, 40, 50, 26, 100)
#     split_chunks <- ceiling(files_to_split_size / (sys_ram * max_percent_ram))
#     
#     # save temporary split files; replace in list of infiles
#     split_files <- mapply(SplitCSVFile, 
#                           csv_file = files_to_split, num_splits = split_chunks, 
#                           MoreArgs = ...,
#                           SIMPLIFY = FALSE)
#     
#     infiles[idx] <- split_files
#     infiles <- unlist(infiles)
#   }
#    
#   return(infiles)
# }



line_first_match <- function(file, pattern, in_chunk_size = 1000) {
  ln <- tryCatch({ system2(command = "grep",
                           args = c("--line-number", 
                                    sprintf("'%s'", pattern), 
                                    file,
                                    "|",
                                    "awk -F  ':' '{print $1}'"),
                           stdout = TRUE) %>% as.integer() },
                 error = function(e) NA,
                 warning = function(e) NA)
  
  if(is.na(ln)) {
    # read in chunks and use grep
    in_chunks <- ceiling(count_lines_in_file(file) / in_chunk_size)
    chunk_fn <- chunk_reader_fn(infile = file, 
                                in_chunks = in_chunks,
                                num_header_rows = 0)
    multiplier <- 0
    while(!is.null(xx <- chunk_fn())) {
      i <- which(grepl(pattern = pattern, x = xx))
      if(length(i) > 0) {
        chunk_fn(force_close = TRUE)
        break;
      }
      
      multiplier <- multiplier + 1
    }
    
    ln <- i + (in_chunk_size * multiplier)
    
  }
  return(ln) 
}


header_row_index_data.table <- function(csv_file, ...) {
  dots <- list(...)
  dots$nrows <- 0
  dots <- c(list(csv_file), dots)
  
  stopifnot(length(csv_file) == 1)
  cols <- colnames(do.call(data.table::fread, dots))
  
  sep <- "[,\t |;:]"
  if("sep" %in% names(dots)) sep <- dots$sep
  
  # header row could be 0 if no header found
  header_row <- line_first_match(file = csv_file,
                                 pattern = paste(cols, collapse = sep))
  return(header_row)
}

header_row_index_readr <- function(csv_file, 
                                   col_names = TRUE, 
                                   skip = 0, 
                                   skip_empty_rows = TRUE,
                                   ...) {
  # skip_empty_rows will skip blank lines at top of file
  num_header_rows <- skip
  
  if(skip_empty_rows) {
    # check for empty header rows
    con = file(csv_file, "r")
    on.exit(close(con))
    
    num_empty_rows <- 0
    xx = readLines(con, n = 1)
    while(length(xx) > 0) {
      xx = readLines(con, n = 1)
      xx <- gsub(pattern = "[[:blank:]]", replacement = "", x = xx)
      if(length(xx) == 1) {
        num_empty_rows <- num_empty_rows + 1
      } else {
        break;
      }
    }
    num_header_rows <- max(num_header_rows, num_empty_rows)
  }
  if(isTRUE(col_names)) num_header_rows <- num_header_rows + 1
  
  return(num_header_rows)
}

header_row_index_laf <- function(csv_file, 
                               header = FALSE, 
                               skip = 0,
                               blank.lines.skip = TRUE,
                               ...) {
  header_row_index_readr(csv_file = csv_file, 
                         col_names = header, 
                         skip = skip, 
                         skip_empty_rows = blank.lines.skip)
}


split_csv_file <- function(infile,
                           in_chunks,
                           chunk_reader = c("bigreadr", "data.table"),
                           ...) {
  dots <- list(...)
  chunk_reader <- match.arg(chunk_reader)
  
  if(.Platform$OS.type != "unix" & chunk_reader == "data.table") {
    warning("csv_to_disk.frame: You have chosen chunk_reader data.table with a non-unix platform. This may fail if certain unix tools are unavailable.")
  } 
  
  
  split_fn <- switch(chunk_reader,
                     data.table = split_csv_file_data.table,
                     bigreadr = split_csv_file_bigreadr,
                     stop("SplitCSVFile: chunk_reader not recognized."))
  
  # SplitCSVFileBigReader needs to know whether there is a header
  dots_split <- dots
  if(chunk_reader == "bigreadr") {
    dots_split <- list(header = ifelse(is.null(dots$col_names), TRUE, dots$col_names))
  }
  dots_split$csv_file <- infile
  dots_split$num_splits <- in_chunks
  infile <- do.call(split_fn, dots_split)
  return(infile)
}

split_csv_file_bigreadr <- function(csv_file, num_splits = 2, header = TRUE) {
  if(num_splits < 2) return(csv_file)
  stopifnot(requireNamespace("bigreadr"),
            isTRUE(header) | isFALSE(header))
  
  n <- bigreadr::nlines(csv_file)
  
  outfile_prefix <- file.path(tempdir(), basename(fs::path_ext_remove(csv_file)))
  split_file_info = bigreadr::split_file(csv_file, 
                                         every_nlines = ceiling(n / num_splits), 
                                         prefix_out = outfile_prefix,
                                         repeat_header = header)
  bigreadr::get_split_files(split_file_info)
}

split_csv_file_data.table <- function(csv_file, num_splits = 2, ...) {
  if(num_splits < 2) return(csv_file)
  
  dots <- list(...)
  
  # use data.table::fread to first identify columns
  # use the column names to identify 1 or more header rows for each csv file
  # split the file and append the header row(s) to the top
  
  cols <- colnames(data.table::fread(csv_file, nrows = 0, ...))
  num_lines <- system2(command = "wc",
                       args = c("--lines",
                                csv_file,
                                "|",
                                "awk -F  ' ' '{print $1}'"),
                       stdout = TRUE) %>% as.integer()
  stopifnot(num_lines > 0)
  
  sep <- "[,\t |;:]"
  if("sep" %in% names(dots)) sep <- dots$sep
  
  # header row could be 0 if no header found
  header_row <- system2(command = "grep",
                        args = c("--line-number", 
                                 sprintf("'%s'", paste(cols, collapse = sep)), 
                                 csv_file,
                                 "|",
                                 "awk -F  ':' '{print $1}'"),
                        stdout = TRUE) %>% as.integer()
  
  temp_dir <- tempdir(check = TRUE)
  body_csv_file <- file.path(temp_dir, "body.csv")
  header_csv_file <- file.path(temp_dir, "header.csv")
  
  
  if(header_row > 0) {
    # separate header row(s) from rest of csv
    
    system2(command = "head",
            args = c("--silent",
                     "--lines", header_row,
                     csv_file,
                     ">>",
                     header_csv_file))
    stopifnot(file.exists(header_csv_file))
    
    system2(command = "tail",
            args = c("--silent",
                     "--lines", num_lines - header_row,
                     csv_file,
                     ">>",
                     body_csv_file))
    stopifnot(file.exists(body_csv_file))
  } 
  
  split_files <- file.path(temp_dir, sprintf("split%d.csv", seq_len(num_splits)))
  mapply(function(split_file, i) {
    system2(command = "split",
            args = c("--number", sprintf("l/%d/%d", i, num_splits),
                     body_csv_file,
                     ">>",
                     split_file))
  },
  split_file = split_files, i = seq_len(num_splits), SIMPLIFY = FALSE)
  
  # now paste back header, if any, and remove unneeded files
  if(header_row > 0) {
    lapply(split_files, function(split_file, header_csv_file) {
      new_file <- file.path(dirname(split_file),
                            paste(basename(split_file), "final", sep = "_"))
      
      system2(command = "cat",
              args = c(header_csv_file, split_file,
                       ">",
                       new_file))
    }, header_csv_file = header_csv_file) 
    
    # move newly generated header + body split files
    fs::file_delete(split_files)
    fs::file_move(path = file.path(dirname(split_files),
                                   paste(basename(split_files), "final", sep = "_")),
                  new_path = split_files)
  }
  
  # rename split files to the csv file name, plus split01, split02, etc.
  # Splits are numbered with minimum 2 digits, maximum equal to number of digits for the number of splits
  # So if splits = 1200, then we will have [csv_name]_split0001.csv, [csv_name]_split0002.csv, ... [csv_name]_split1200.csv
  num_digits <- max(2, nchar(as.character(num_splits)))
  file_name <- rlang::parse_expr(sprintf('sprintf("split%%0%dd", seq_along(split_files))',  num_digits))
  outfiles <- file.path(dirname(split_files), 
                        paste(fs::path_ext_remove(basename(csv_file)),
                              paste(eval(expr(!! file_name)), "csv", sep = "."),
                              sep = "_"))
  
  fs::file_move(path = split_files,
                new_path = outfiles)
  
  file.remove(header_csv_file, body_csv_file)
  
  stopifnot(file.exists(outfiles),
            length(outfiles) == num_splits)
  
  return(outfiles)
}

# number files in order from filename_suffix01, prefix_suffix02, etc.
# numbering increased with number of files. 
# so if 1000 files, number prefix_suffix0001, prefix_suffix0002, ..., prefix_suffix1000
renumber_file_names <- function(files, suffix = "", extension = NULL) {
  num_files <- length(files)
  
  num_digits <- max(2, nchar(as.character(num_files)))
  file_name <- rlang::parse_expr(sprintf('sprintf("%s%%0%dd", seq_along(files))', suffix, num_digits))
  sep <- "."
  if(is.null(extension)) sep <- ""
  outfiles <- file.path(dirname(files), 
                        paste(fs::path_ext_remove(basename(files)),
                              paste(eval(expr(!! file_name)), extension, sep = sep),
                              sep = "_"))
  outfiles
}

# renumber_file_names(LETTERS)
# renumber_file_names(LETTERS, suffix = "split")
# renumber_file_names(LETTERS, suffix = "split", extension = "csv")

# classes could vary between files.
# determine the controlling class based on the data.table list: 
# logical, integer, integer64, double, character
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
