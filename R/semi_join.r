#' @param x a disk.frame
#' @param y a data.frame or disk.frame. If data.frame then returns lazily; if disk.frame it performs the join eagerly and return a disk.frame
#' @param outdir output directory for disk.frame
#' @rdname join 
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' join.df = semi_join(cars.df, cars.df)
#' 
#' # clean up cars.df
#' delete(cars.df)
#' delete(join.df)
semi_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_semi_join"), merge_by_chunk_id = FALSE, overwrite = TRUE, .progress = FALSE) {
  stopifnot("disk.frame" %in% class(x))
  
  overwrite_check(outdir, overwrite)
  
  if("data.frame" %in% class(y)) {
    tmp = cmap_dfr(x, ~{
      semi_join(.x, y, by = by, copy = copy, ...)
    }, .progress = .progress)
    
    return(tmp)
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
      x = rechunk(x, by, nchunks = max(ncy,ncx), outdir=tempfile(fileext = ".jdf"), overwrite = FALSE)
      y = rechunk(y, by, nchunks = max(ncy,ncx), outdir=tempfile(fileext = ".jdf"), overwrite = FALSE)
      return(semi_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = TRUE, overwrite = overwrite, .progress = .progress))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      res = cmap2.disk.frame(x, y, ~{
        if(is.null(.y)) {
          return(data.table())
        } else if (is.null(.x)) {
          return(data.table())
        }
        semi_join(.x, .y, by = by, copy = copy, ..., overwrite = overwrite)
      }, outdir = outdir, .progress = .progress)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}
