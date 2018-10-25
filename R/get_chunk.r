get_chunk <- function(...) {
  UseMethod("get_chunk")
}


get_chunk.disk.frame <- function(df, n, keep = NULL, full.name = F) {
  stopifnot("disk.frame" %in% class(df))
  
  path = attr(df,"path")
  keep1 = attr(df,"keep")
  
  #browser()
  cmds = attr(df,"lazyfn")
  
  if(!is.null(keep1)) {
    keep = intersect(keep1, keep)
    if (!all(keep %in% keep1)) {
      warning("some of the variables specified in keep is not available")
    }
  }
  
  if(is.numeric(n)) {
    filename = dir(path,full.names = T)[n]
  } else {
    if (full.name) {
      filename = n
    } else {
      filename = file.path(path, n)
    }
  }
  
  if (is.null(cmds)) {
    read_fst(filename, columns = keep, as.data.table = T)
  } else {
    play(read_fst(filename, columns = keep, as.data.table = T), cmds)
  }
}
