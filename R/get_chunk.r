get_chunk.disk.frame <- function(df, n, keep = NULL) {
  stopifnot("disk.frame" %in% class(df))
  
  path = attr(df,"path")
  keep1 = attr(df,"keep")
  
  #browser()
  fn = attr(df,"lazyfn")
  
  if(!is.null(keep1)) {
    keep = intersect(keep1, keep)
    if (!all(keep %in% keep1)) {
      warning("some of the variables specified in keep is not available")
    }
  }
    
  if (is.null(fn)) {
    read_fst(dir(path,full.names = T)[n], columns = keep, as.data.table = T)
  } else {
    fn(read_fst(dir(path,full.names = T)[n], columns = keep, as.data.table = T)) 
  }
}
