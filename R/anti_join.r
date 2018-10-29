#' Performs anti_join
#' @param x a disk.frame
#' @param y a data.frame or disk.frame. If data.frame then returns lazily; if disk.frame it performs the join eagerly and return a disk.frame
#' @param outdir output directory for disk.frame
#' @rdname join
#' @export
anti_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_anti_join"), merge_by_chunk_id = F) {
  stopifnot("disk.frame" %in% class(x))
  
  if("data.frame" %in% class(y)) {
    # note that x is named .data in the lazy evaluation
    .data <- x
    cmd <- lazyeval::lazy(anti_join(.data, y, by, copy, ...))
    return(record(.data, cmd))
  } else if("disk.frame" %in% class(y)) {
    if(is.null(merge_by_chunk_id)) {
      stop("both x and y are disk.frames. You need to specify merge_by_chunk_id = TRUE or FALSE explicitly")
    }
    if(is.null(by)) {
      by <- intercept(names(x), names(y))
    }
    
    ncx = nchunks(x)
    ncy = nchunks(y)
    if (merge_by_chunk_id == F) {
      warning("merge_by_chunk_id = FALSE. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making y a data.frame or set merge_by_chunk_id = TRUE for better performance.")
      x = hard_group_by(x, by, nchunks = max(ncy,ncx))
      y = hard_group_by(y, by, nchunks = max(ncy,ncx))
      return(anti_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = T))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      res = map_by_chunk_id(x, y, ~{
        if(is.na(.y)) {
          return(.x)
        } else if (is.na(.x)) {
          return(data.table())
        }
        anti_join(.x, .y, by = by, copy = copy, ...)
      }, outdir = outdir)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}
