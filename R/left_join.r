left_join_y_is_data.frame = create_chunk_mapper(dplyr::left_join)

#' Performs join/merge for disk.frames
#' @rdname join
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' join.df = left_join(cars.df, cars.df)
#' 
#' # clean up cars.df
#' delete(cars.df)
#' delete(join.df)
left_join.disk.frame = function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), ..., keep=FALSE, outdir = tempfile("tmp_disk_frame_left_join"), merge_by_chunk_id = FALSE, overwrite = TRUE, .progress = FALSE) {
  stopifnot("disk.frame" %in% class(x))
  
  if ("data.frame" %in% class(y)) {
    left_join_y_is_data.frame(x, y, by=by, copy=copy, suffix=suffix, ..., keep=keep)
  } else {
    if(is.null(merge_by_chunk_id)) {
      stop("Both `x` and `y` are disk.frames. You need to specify `merge_by_chunk_id = TRUE` or `FALSE` explicitly")
    }
    if(is.null(by)) {
      by <- intersect(names(x), names(y))
    }
    
    ncx = nchunks(x)
    ncy = nchunks(y)
    if (merge_by_chunk_id == FALSE) {
      warning("`merge_by_chunk_id = FALSE`. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making `y` a data.frame or set merge_by_chunk_id = TRUE for better performance.")
      x = rechunk(x, nchunks = max(ncy, ncx), shardby = by, outdir=tempfile(), overwrite = FALSE)
      y = rechunk(x, nchunks = max(ncy, ncx), shardby = by, outdir=tempfile(), overwrite = FALSE)
      return(left_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = TRUE, overwrite = overwrite, .progress = .progress))
    } else if(merge_by_chunk_id == TRUE) {
      dotdotdot = list(...)
      res = cmap2.disk.frame(x, y, ~{
        if(is.null(.y)) {
          return(.x)
        } else if (is.null(.x)) {
          return(data.table())
        }
        left_join(.x, .y, by = by, copy = copy, suffix=suffix, ..., keep=keep)
        #llj = purrr::lift(dplyr::left_join)
        #llj(c(list(x=.x, y =.y, by = by, copy = copy), dotdotdot))
      }, outdir = outdir)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}
  