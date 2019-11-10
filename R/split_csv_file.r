#' Split a file into chunks
#' 
#' Split a csv file into multiple parts. 
#' Uses either bigreadr or data.table to split the file, adding the header to each split.
#' @param infile One or more csv files to split
#' @param num_splits Number of splits for each file. If more than one infile, can specify either
#' a single num_splits value to apply to every file, or a vector of integers with a length 
#' equal to the number of infiles.
#' @param header Either TRUE, FALSE, or an integer specifying the header index.
#' If TRUE, the first line of the file will be copied to each file split.
#' If an integer is provided, all lines above or equal to that integer 
#' will be copied to each file split.
#' @param split_method If unix, can use \code{\link[data.table]{fread}} to guess at the header location.
#' If bigreadr, must specify either header TRUE or FALSE. 
#' \code{\link[bigreadr]{split_file}} used to split the file.
#' If header is not specified, it will be assumed to be TRUE.
#' Note that unix relies on unix tools that may not be available on a Windows platform.
#' If the header is not known, try using \code{\link{header_row_index}} to determine the correct header. 
#' @param outdir Where to save the split files.
#' @param suffix Suffix to use when naming the split files.
#' @return Vector of file paths for the split files. 
#' 
#' @importFrom fs path_ext
#' @importFrom fs file_move
#' @importFrom future.apply future_mapply
#' @export
split_csv_file <- function(infile,
                           num_splits = 2,
                           header = TRUE,
                           split_method = c("bigreadr", "unix"),
                           outdir = tempdir(),
                           suffix = "split") {
  if(length(infile) > 1) return(future.apply::future_mapply(split_csv_file,
                                       infile = infile,
                                       num_splits = num_splits,
                                       split_method = split_method,
                                       header = header,
                                       outdir = outdir,
                                       suffix = suffix,
                                       SIMPLIFY = FALSE))
  
  if(num_splits < 2) return(infile)
  
  split_method <- match.arg(split_method)
  if(!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
  
  split_files <- switch(split_method,
                unix = split_csv_file_unix(csv_file = infile,
                                           outdir = outdir,
                                           num_splits = num_splits,
                                           header_row_index = header),
                
                bigreadr = split_csv_file_bigreadr(csv_file = infile,
                                                   outdir = outdir,
                                                   num_splits = num_splits,
                                                   header = header))
  
  # rename files for convenience
  extension <- fs::path_ext(infile)
  new_path <- renumber_file_names(files = file.path(dirname(split_files), basename(infile)), 
                                  suffix = "split", 
                                  extension = extension) 
  
  out <- fs::file_move(path = split_files, 
                       new_path = new_path)
  stopifnot(file.exists(out))
  
  return(out)
}

#' Split csv file using \code{\link[bigreadr]{split_file}}
#' Requires that header is either TRUE or FALSE.
#' Does not handle blank spaces or other text before the header.
#' @seealso \code{\link[bigreadr]{nlines}}, 
#'          \code{\link[bigreadr]{split_file}}, 
#'          \code{\link[bigreadr]{get_split_files}}
#' @keywords internal
split_csv_file_bigreadr <- function(csv_file, outdir = tempdir(), num_splits = 2, header = TRUE) {
  stopifnot(requireNamespace("bigreadr"))
  if(num_splits < 2) return(csv_file)
  
  if(header == 1) header <- TRUE
  if(header == 0) header <- FALSE
  
  stopifnot(requireNamespace("bigreadr"),
            isTRUE(header) | isFALSE(header))
  
  if(!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
  
  n <- bigreadr::nlines(csv_file)
  
  outfile_prefix <- file.path(outdir, basename(fs::path_ext_remove(csv_file)))
  split_file_info = bigreadr::split_file(csv_file, 
                                         every_nlines = ceiling(n / num_splits), 
                                         prefix_out = outfile_prefix,
                                         repeat_header = header)
  bigreadr::get_split_files(split_file_info)
}

#' Split csv file using unix tools
#' Can split a file with any sized header. 
#' @importFrom fs file_delete
#' @importFrom fs file_move
#' @importFrom future.apply future_mapply
#' @keywords internal
split_csv_file_unix <- function(csv_file, outdir = tempdir(), num_splits = 2, header_row_index = 1) {
  if(num_splits < 2) return(csv_file)
  if(!dir.exists(outdir)) dir.create(outdir, recursive = TRUE)
  
  header_row_index <- as.integer(header_row_index)
  
  
  body_csv_file <- file.path(outdir, "body.csv")
  header_csv_file <- file.path(outdir, "header.csv")
  
  if(header_row_index > 0) {
    # separate header row(s) from rest of csv
    
    system2(command = "head",
            args = c("--silent",
                     "--lines", header_row_index,
                     csv_file,
                     ">>",
                     header_csv_file))
    stopifnot(file.exists(header_csv_file))
    
    num_lines <- count_lines_in_file(csv_file)
    
    system2(command = "tail",
            args = c("--silent",
                     "--lines", num_lines - header_row_index,
                     csv_file,
                     ">>",
                     body_csv_file))
    stopifnot(file.exists(body_csv_file))
  } 
  
  split_files <- file.path(outdir, sprintf("split%d.csv", seq_len(num_splits)))
  future.apply::future_mapply(function(split_file, i) {
    system2(command = "split",
            args = c("--number", sprintf("l/%d/%d", i, num_splits),
                     body_csv_file,
                     ">>",
                     split_file))
  },
  split_file = split_files, i = seq_len(num_splits), SIMPLIFY = FALSE)
  
  # now paste back header, if any, and remove unneeded files
  if(header_row_index > 0) {
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
  
  fs::file_delete(c(header_csv_file, body_csv_file))
  return(split_files)
}

#' Get header row
#' 
#' Determine the line number for the header row in a file.
#' Uses a simplistic method of matching the column names to find the header row.
#' @param csv_files A single csv file to examine.
#' @param method Method to use to determine the column names. 
#' data.table uses \code{\link[data.table]{fread}}. 
#' readr uses \code{\link[readr]{read_delim}}.
#' LaF uses \code{\link[LaF]{detect_dm_csv}}
#' @param ... Additional arguments to pass to the method.
#' @return Line number for the header if found; 0 otherwise.
#' @importFrom future.apply future_mapply
#' @export
header_row_index <- function(csv_files, method = c("data.table", "readr", "LaF"), ...) {
  dots <- list(...)
  
  method <- match.arg(method)
  
  col_types <- switch(method,
                      data.table = determine_classes_csv_data.table(csv_files, ...),
                      readr = determine_classes_csv_readr(csv_files, ...),
                      LaF = determine_classes_csv_laf(csv_files, ...))
  cols <- colnames(col_types)
    
  sep <- "[,\t |;:]"
  if("sep" %in% names(dots)) sep <- dots$sep
  if("delim" %in% names(dots)) sep <- dots$delim
  
  header_row <- future_mapply(line_first_match, 
                              file = csv_files,
                              MoreArgs = list(pattern = paste(cols, collapse = sep)))
  return(header_row)
}

#' Look for a line that matches the pattern in the file.
#' Will first try using the unix grep command to search the file.
#' If that fails, it will fall back on reading in chunks of the file.
#' @keywords internal
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
  if(length(ln) == 0) return(0)
  ln <- ln[[1]]
  if(is.na(ln)) {
    # read in chunks and use grep
    nchunks <- ceiling(count_lines_in_file(file) / in_chunk_size)
    chunk_fn <- chunk_reader_fn(infile = file, 
                                nchunks = nchunks,
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

#' Count number of lines in file
#' First tries unix command wc.
#' If that fails, it tries line count functions from different packages.
#' Last resort is readLines, which could fail if the file size is large, 
#' because it requires reading it into memory. Chunks are used to attempt to avoid this.
#' @seealso \code{\link[bigreadr]{nlines}}, 
#'          \code{\link[LaF]{determine_nlines}}, 
#'          \code{\link[R.utils]{countLines}}
#' @keywords internal
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
      num_lines <- LaF::determine_nlines(file)
    } else if(requireNamespace("R.utils")) {
      num_lines <- R.utils::countLines(file)
    } else {
      nchunks <- estimate_chunks_needed(file)
      chunk_fn <- chunk_reader_fn(infile = file, 
                                  nchunks = nchunks,
                                  num_header_rows = 0)
      num_lines <- 0
      while(!is.null(xx <- chunk_fn())) {
        num_lines <- num_lines + length(xx)
      }
    }
  }
  stopifnot(num_lines > 0)
  return(num_lines)
}

#' number files in order from filename_suffix01, prefix_suffix02, etc.
#' numbering increased with number of files. 
#' so if 1000 files, number prefix_suffix0001, prefix_suffix0002, ..., prefix_suffix1000
#' Example:
#' renumber_file_names(LETTERS)
#' renumber_file_names(LETTERS, suffix = "split")
#' renumber_file_names(LETTERS, suffix = "split", extension = "csv")
#' @importFrom rlang parse_expr
#' @importFrom rlang expr
#' @importFrom fs path_ext_remove
#' @keywords internal
renumber_file_names <- function(files, suffix = "", extension = NULL) {
  num_files <- length(files)
  
  num_digits <- max(2, nchar(as.character(num_files)))
  file_name <- rlang::parse_expr(sprintf('sprintf("%s%%0%dd", seq_along(files))', suffix, num_digits))
  sep <- "."
  if(is.null(extension)) sep <- ""
  outfiles <- file.path(dirname(files), 
                        paste(fs::path_ext_remove(basename(files)),
                              paste(eval(rlang::expr(!! file_name)), extension, sep = sep),
                              sep = "_"))
  outfiles
}




