#' @rdname join
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' join.df = full_join(cars.df, cars.df, merge_by_chunk_id = TRUE)
#' 
#' # clean up cars.df
#' delete(cars.df)
#' delete(join.df)
full_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_full_join"), overwrite = TRUE, merge_by_chunk_id, .progress = FALSE) {
  stopifnot("disk.frame" %in% class(x))
  
  overwrite_check(outdir, overwrite)
  
  
  if("data.frame" %in% class(y)) {
    # full join cannot be support for y in data.frame
    ncx = nchunks(x)
    dy = shard(y, shardby = by, nchunks = ncx, overwrite = TRUE)
    dx = hard_group_by(x, by = by, overwrite = TRUE)
    return(full_join.disk.frame(dx, dy, by, copy=copy, outdir=outdir, merge_by_chunk_id = TRUE))
  } else if("disk.frame" %in% class(y)) {
    if(is.null(merge_by_chunk_id)) {
      stop("both x and y are disk.frames. You need to specify merge_by_chunk_id = TRUE or FALSE explicitly")
    }
    if(is.null(by)) {
      by <- intersect(names(x), names(y))
    }
    
    ncx = nchunks(x)
    ncy = nchunks(y)
    if (merge_by_chunk_id == FALSE) {
      warning("merge_by_chunk_id = FALSE. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making y a data.frame or set merge_by_chunk_id = TRUE for better performance.")
      x = hard_group_by(x, by, nchunks = max(ncy,ncx), overwrite = TRUE)
      y = hard_group_by(y, by, nchunks = max(ncy,ncx), overwrite = TRUE)
      return(full_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = TRUE, overwrite = overwrite, .progress = .progress))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      res = map2(x, y, ~{
        if(is.null(.y)) {
          return(.x)
        } else if (is.null(.x)) {
          return(.y)
        }
        full_join(.x, .y, by = by, copy = copy)
      }, outdir = outdir, overwrite = overwrite, .progress = .progress)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}
