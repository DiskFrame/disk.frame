#' Performs semi join
#' @param x a disk.frame
#' @param y a data.frame or disk.frame. If data.frame then returns lazily; if disk.frame it performs the join eagerly and return a disk.frame
#' @param outdir output directory for disk.frame
#' @rdname join 
#' @export
semi_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., merge_by_chunk_id) {
  stopifnot("disk.frame" %in% class(x))
  
  if("data.frame" %in% class(y)) {
    # note that x is named .data in the lazy evaluation
    .data <- x
    cmd <- lazyeval::lazy(semi_join(.data, y, by, copy, ...))
    return(record(.data, cmd))
  } else if("disk.frame" %in% class(y)) {
    #list.files(
    ncx = nchunks(x)
    ncy = nchunks(y)
    if (merge_by_chunk_id == F) {
      warning("merge_by_chunk_id = FALSE. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making y a data.frame or set merge_by_chunk_id = TRUE for better performance.")
      x = hard_group_by(x, by, nchunks = max(ncy,ncx))
      y = hard_group_by(y, by, nchunks = max(ncy,ncx))
      return(semi_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = T))
    } else if (merge_by_chunk_id) {
      res = map_by_chunk_id(x, y, ~{
        if(is.na(.y)) {
          return(data.table())
        } else if (is.na(.x)) {
          return(data.table())
        }
        semi_join(.x, .y, by = by, copy = copy, ...)
      }, outdir = outdir)
      return(res)
    }
  }
}
