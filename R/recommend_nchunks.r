#' Recommend number of chunks based on input size
#' @description Computes the recommended number of chunks to break a data.frame
#' into. It can accept filesizes in bytes (as integer) or a data.frame
#' @param df a disk.frame or the file size in bytes of a CSV file holding the
#'   data
#' @param type only = "csv" is supported. It indicates the file type
#'   corresponding to file size `df`
#' @param minchunks the minimum number of chunks. Defaults to the number of CPU
#'   cores (without hyper-threading)
#' @param conservatism a multiplier to the recommended number of chunks. The
#'   more chunks the smaller the chunk size and more likely that each chunk can
#'   fit into RAM
#' @param ram_size The amount of RAM available which is usually computed. Except on RStudio with R3.6+
#' @importFrom pryr object_size
#' @importFrom utils memory.limit
#' @importFrom benchmarkme get_ram
#' @export
#' @examples
#' # recommend nchunks based on data.frame
#' recommend_nchunks(cars)
#'
#' # recommend nchunks based on file size ONLY CSV is implemented at the moment
#' recommend_nchunks(1024^3)
recommend_nchunks <- function(df, type = "csv", minchunks = parallel::detectCores(logical = FALSE), conservatism = 2, ram_size = NULL) {
  dfsize = 0
  if ("data.frame" %in% class(df)) {
    # the df's size in gigabytes
    dfsize = as.numeric(pryr::object_size(df))/1024/1024/1024
  } else if (is.numeric(df) & type == "csv") {
    # assume that df is the estimated number of bytes of the data
    dfsize = df/1024/1024/1024
  } else {
    dfsize = df/1024/1024/1024
  }
  
  if (is.null(ram_size)) {
    if(.Platform$GUI == "RStudio") {
      majorv = as.integer(version$major)
      minorv = as.integer(strsplit(version$minor, ".", fixed=TRUE)[[1]][1])
      if(majorv>=3 & minorv >= 6) {
        if(.Platform$OS.type == "windows") {
          ram_size = getOption("disk.frame.ram_size")
        }
        if (is.null(ram_size)) {
          message("You are running RStudio with R 3.6 on Windows. There is a bug with memory detection.")
          message("The option disk.frame.ram_size is not set. Going to assume your ram_size is 16 (gigabyte)")
          message("To set the ram_size, do options(disk.frame_ram_size = your_ram_size_in_gigabytes)")
          ram_size = 16
        } 
      }
    }
    
    # the amount of memory available in gigabytes
    if (Sys.info()[["sysname"]] == "Windows") {
      ram_size = memory.limit() / 1024
      #} else if (Sys.info()[["sysname"]] %in% c("Linux","Darwin")) {
    } else {
      #ram_size = as.numeric(system('grep MemTotal /proc/meminfo', ignore.stdout = TRUE) / 1024)
      ram_size = benchmarkme::get_ram()/1024/1024/1024
    } 
    
    if(is.na(ram_size)) {
      warning("memory size not detected, Assumming you have at least 16G of RAM")
      ram_size = 16
    }
    # assume at least 1G of RAM
    ram_size = max(ram_size, 1, na.rm = TRUE)
    
  }
    
  # the number physical cores not counting hyper threaded ones as 2; they are counted as 1
  nc = parallel::detectCores(logical = FALSE)
  
  
  max(round(dfsize/ram_size*nc)*nc*conservatism, minchunks)
}