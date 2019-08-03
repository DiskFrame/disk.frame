#' @export
#' @rdname join
inner_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., outdir = tempfile("tmp_disk_frame_inner_join"), merge_by_chunk_id = NULL, overwrite = TRUE) {
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
    quo_dotdotdot = enquos(...)
    res = map_dfr(x, ~{
      code = quo(inner_join(.x, y, by = by, copy = copy, !!!quo_dotdotdot))
      rlang::eval_tidy(code)
    })
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
      x = hard_group_by(x, by, nchunks = max(ncy,ncx), overwrite = T)
      y = hard_group_by(y, by, nchunks = max(ncy,ncx), overwrite = T)
      return(inner_join.disk.frame(x, y, by, outdir = outdir, merge_by_chunk_id = TRUE, overwrite = overwrite))
    } else if ((identical(shardkey(x)$shardkey, "") & identical(shardkey(y)$shardkey, "")) | identical(shardkey(x), shardkey(y))) {
      dotdotdot <- list(...)
      
      res = map2.disk.frame(x, y, ~{
        #browser()
        if(is.null(.y)) {
          return(data.table())
        } else if (is.null(.x)) {
          return(data.table())
        }
        #inner_join(.x, .y, by = by, copy = copy, ..., overwrite = overwrite)
        lij = purrr::lift(dplyr::inner_join)
        lij(c(list(x = .x, y = .y, by = by, copy = copy), dotdotdot))
      }, outdir = outdir)
      return(res)
    } else {
      # TODO if the shardkey are the same and only the shardchunks are different then just shard again on one of them is fine
      stop("merge_by_chunk_id is TRUE but shardkey(x) does NOT equal to shardkey(y). You may want to perform a hard_group_by() on both x and/or y or set merge_by_chunk_id = FALSE")
    }
  }
}