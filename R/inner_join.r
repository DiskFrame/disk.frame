#' @export
#' @rdname join
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' join.df = inner_join(cars.df, cars.df, merge_by_chunk_id = TRUE)
#' 
#' # clean up cars.df
#' delete(cars.df)
#' delete(join.df)
inner_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), ..., keep=FALSE, outdir = tempfile("tmp_disk_frame_inner_join"), merge_by_chunk_id = NULL, overwrite = TRUE, .progress = FALSE) {
  stopifnot("disk.frame" %in% class(x))
  
  overwrite_check(outdir, overwrite)
  
  
  if(!is.null(outdir)) {
    if(overwrite & fs::dir_exists(outdir)) {
      fs::dir_delete(outdir)
      fs::dir_create(outdir)
    } else {
      fs::dir_create(outdir)
    }
  }
  
  if("data.frame" %in% class(y)) {
    res = cmap_dfr(x, ~{
      inner_join(.x, y, by = by, copy = copy, suffix=suffix, ..., keep=keep)
    }, .progress = .progress)
    return(res)
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
      x = rechunk(x, shardby=by, nchunks = max(ncy,ncx), outdir = tempfile(fileext = ".df"), overwrite = FALSE)
      y = rechunk(y, shardby=by, nchunks = max(ncy,ncx), outdir = tempfile(fileext = ".df"), overwrite = FALSE)
      return(inner_join.disk.frame(x, y, by, copy=copy, suffix = suffix, ..., keep=keep, outdir = outdir, merge_by_chunk_id = TRUE, overwrite = overwrite))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      res = cmap2.disk.frame(x, y, ~{
        if(is.null(.y)) {
          return(data.table())
        } else if (is.null(.x)) {
          return(data.table())
        }
        inner_join(.x, .y, by = by, copy = copy, suffix = suffix, ..., keep=keep)
      }, outdir = outdir, .progress = .progress, overwrite = overwrite)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}