#' Computes the recommended number of chunks to break a data.frame into
#' @param df a disk.frame or the file size in bytes of a CSV file holding the data
#' @param type only = "csv" is supported. It indicates the file type corresponding to file size `df`
#' @param minchunks the minimum number of chunks. Defaults to the number of CPU cores (without hyper-threading)
#' @param conservatism a multiplier to the recommended number of chunks. The more chunks the smaller the chunk size and more likely that each chunk can fit into RAM
#' @importFrom pryr object_size
#' @importFrom utils memory.limit
#' @export
recommend_nchunks <- function(df, type = "csv", minchunks = parallel::detectCores(logical = F), conservatism = 2) {
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
  } else {
    ml = system('grep MemTotal /proc/meminfo')
  }
    
  # the number physical cores not counting hyper threaded ones as 2; they are counted as 1
  nc = parallel::detectCores(logical = F)
  
  
  max(round(dfsize/ml*nc)*nc*conservatism, minchunks)
}