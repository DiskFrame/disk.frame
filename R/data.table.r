#' [ interface for disk.frame using fst backend
#' @param df a disk.frame
#' @param i same as data.table
#' @param j same as data.table
#' @param ... same as data.table
#' @param keep the columns to srckeep
#' @import fst 
#' @importFrom future.apply future_lapply
#' @importFrom data.table rbindlist 
#' @export
`[.disk.frame` <- function(df, i, j,..., keep = NULL) {
  res <- NULL
  fpath <- attr(df,"path")
  
  ff <- list.files(attr(df,"path"))
  
  i = deparse(substitute(i))
  j = deparse(substitute(j))
  dotdot = deparse(substitute(...))
  
  keep_for_future = keep
  
  res <- future.apply::future_lapply(ff, function(k,i,j,dotdot) {
    # sometimes the i and j an dotdot comes in the form of a vector so need to paste them together
    j = paste0(j,collapse="")
    dotdot = paste0(dotdot,collapse="")
    i = paste0(i,collapse="")
    
    if(dotdot == "NULL") {
      code = sprintf("a[%s,%s]", i, j)
    } else if (j == "NULL") {
      code = sprintf("a[%s]", i)
    } else {
      code = sprintf("a[%s,%s,%s]", i, j, dotdot)
    }
    a = get_chunk.disk.frame(df, k, keep = keep_for_future)
    
    aa <- eval(parse(text=code))
    
    rm(a); gc()
    aa
  }, i, j, dotdot)
  
  # sometimes the returned thing is a vetor e.g. df[,.N]
  if("data.frame" %in% class(res[[1]])) {
    return(rbindlist(res))
  } else if(is.vector(res)) {
    return(unlist(res, recursive = F))
  } else {
    warning("spooky")
    return(res)
  }
}