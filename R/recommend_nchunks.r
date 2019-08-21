#' Recommend number of chunks based on input size
#' @description
#' Computes the recommended number of chunks to break a data.frame into. It can
#' accept filesizes in bytes (as integer) or a data.frame
#' @param df a disk.frame or the file size in bytes of a CSV file holding the data
#' @param type only = "csv" is supported. It indicates the file type corresponding to file size `df`
#' @param minchunks the minimum number of chunks. Defaults to the number of CPU cores (without hyper-threading)
#' @param conservatism a multiplier to the recommended number of chunks. The more chunks the smaller the chunk size and more likely that each chunk can fit into RAM
#' @importFrom pryr object_size
#' @importFrom utils memory.limit
#' @export
#' @examples
#' # recommend nchunks based on data.frame
#' recommend_nchunks(cars)
#' 
#' # recommend nchunks based on file size ONLY CSV is implemented at the moment
#' recommend_nchunks(1024^3)
recommend_nchunks <- function(df, type = "csv", minchunks = parallel::detectCores(logical = FALSE), conservatism = 2) {
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
  

  # the amount of memory available in gigabytes
  if (Sys.info()[["sysname"]] == "Windows") {
    ml = memory.limit() / 1024
  #} else if (Sys.info()[["sysname"]] %in% c("Linux","Darwin")) {
  } else {
    #ml = as.numeric(system('grep MemTotal /proc/meminfo', ignore.stdout = TRUE) / 1024)
    ml = benchmarkme::get_ram()/1024/1024/1024
  } 
  
  if(is.na(ml)) {
    warning("memory size not detected, Assumming you have at least 16G of RAM")
    ml = 16
  }
  # assume at least 1G of RAM
  ml = max(ml, 1, na.rm = TRUE)
    
  # the number physical cores not counting hyper threaded ones as 2; they are counted as 1
  nc = parallel::detectCores(logical = FALSE)
  
  
  max(round(dfsize/ml*nc)*nc*conservatism, minchunks)
}