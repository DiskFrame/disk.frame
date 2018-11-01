#' Perfomrs inner_join
#' @param x a disk.frame
#' @param y a data.frame or disk.frame. If data.frame then returns lazily; if disk.frame it performs the join eagerly and return a disk.frame
#' @param outdir output directory for disk.frame
#' @export
inner_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_inner_join"), merge_by_chunk_id = NULL, overwrite = T) {
  stopifnot("disk.frame" %in% class(x))
  
  if(!is.null(outdir)) {
    if(overwrite & fs::dir_exists(outdir)) {
      fs::dir_delete(outdir)
      fs::dir_create(outdir)
    } else {
      fs::dir_create(outdir)
    }
  }
  
  if("data.frame" %in% class(y)) {
    # note that x is named .data in the lazy evaluation
    .data <- x
    cmd <- lazyeval::lazy(inner_join(.data, y, by, copy, ...))
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
      x = hard_group_by(x, by, nchunks = max(ncy,ncx), overwrite = T)
      y = hard_group_by(y, by, nchunks = max(ncy,ncx), overwrite = T)
      return(inner_join.disk.frame(x, y, by, outdir = outdir, merge_by_chunk_id = T, overwrite = overwrite))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      res = map_by_chunk_id(x, y, ~{
        if(is.na(.y)) {
          return(data.table())
        } else if (is.na(.x)) {
          return(data.table())
        }
        inner_join(.x, .y, by = by, copy = copy, ..., overwrite = overwrite)
      }, outdir = outdir)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}