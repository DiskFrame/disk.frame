#' @param by join by
#' @param copy same as dplyr::anti_join
#' @param merge_by_chunk_id the merge is performed by chunk id
#' @param overwrite overwrite output directory
#' @rdname join
#' @importFrom rlang quo enquos
#' @importFrom dplyr anti_join left_join full_join semi_join inner_join
#' @return disk.frame
#' @export
anti_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_anti_join"), merge_by_chunk_id = F, overwrite = T) {
  #browser()
  stopifnot("disk.frame" %in% class(x))
  
  overwrite_check(outdir, overwrite)
  
  if("data.frame" %in% class(y)) {
    quo_dotdotdot = enquos(...)
    map_dfr(x, ~{
      code = quo(anti_join(.x, y, by = by, copy = copy, !!!quo_dotdotdot))
      rlang::eval_tidy(code)
    })
  } else if("disk.frame" %in% class(y)) {
    if(is.null(merge_by_chunk_id)) {
      stop("both x and y are disk.frames. You need to specify merge_by_chunk_id = TRUE or FALSE explicitly")
    }
    if(is.null(by)) {
      by <- intersect(names(x), names(y))
    }
    
    ncx = nchunks(x)
    ncy = nchunks(y)
    if (merge_by_chunk_id == F) {
      warning("merge_by_chunk_id = FALSE. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making y a data.frame or set merge_by_chunk_id = TRUE for better performance.")
      
      x = hard_group_by(x, by, nchunks = max(ncy,ncx), overwrite = T)
      y = hard_group_by(y, by, nchunks = max(ncy,ncx), overwrite = T)
      return(anti_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = T, overwrite = overwrite))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      #res = map2.disk.frame(x, y, ~{
      res = map2(x, y, ~{
        if(is.null(.y)) {
          return(.x)
        } else if (is.null(.x)) {
          return(data.table())
        }
        anti_join(.x, .y, by = by, copy = copy, ..., overwrite = overwrite)
      }, outdir = outdir)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}
