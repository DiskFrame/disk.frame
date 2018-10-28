#' Computes the recommended number of chunks to break a data.frame into
#' @import pryr
#' @export
recommend_nchunks <- function(df) {
  dfsize = 0
  if ("data.frame" %in% class(df)) {
    # the df's size in gigabytes
    dfsize = as.numeric(pryr::object_size(df))/1024/1024/1024
  } else if (is.numeric(df)) {
    # assume that df is the estimated number of bytes of the data
    dfsize = dfsize/1024/1024/1024
  }
  

  # the amount of memory available in gigabytes
  ml = memory.limit() / 1024
  
  # the number physical cores not counting hyper threaded ones as 2; they are counted as 1
  nc = parallel::detectCores(logical = F)
  
  
  max(round(dfsize/ml*nc*4)*nc, nc)
}