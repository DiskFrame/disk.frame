#' Deliberately NOT implemented. The users should use left_join instead
#' @param x a disk.frame
#' @param y a data.frame or disk.frame. If data.frame then returns lazily; if disk.frame it performs the join eagerly and return a disk.frame
#' @param outdir output directory for disk.frame
#' @rdname join
#right_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ..., merge_by_chunk_id) {
  # stopifnot("disk.frame" %in% class(y))
  # 
  # if("data.frame" %in% class(x)) {
  #   # note that x is named .data in the lazy evaluation
  #   .data <- y
  #   cmd <- lazyeval::lazy(right_join(x, .data, by, copy, ...))
  #   return(record(.data, cmd))
  # } else if("disk.frame" %in% class(y)) {
  #   #list.files(
  #   ncx = nchunks(x)
  #   ncy = nchunks(y)
  #   if (merge_by_chunk_id == F) {
  #     warning("merge_by_chunk_id = FALSE. This will take significantly longer and the preparations needed are performed eagerly which may lead to poor performance. Consider making x a data.frame or set merge_by_chunk_id = TRUE for better performance.")
  #     x = hard_group_by(x, by, nchunks = max(ncy,ncx))
  #     y = hard_group_by(y, by, nchunks = max(ncy,ncx))
  #     return(right_join.disk.frame(x, y, by, copy = copy, outdir = outdir, merge_by_chunk_id = T))
  #   } else if (merge_by_chunk_id) {
  #     res = map_by_chunk_id(x, y, ~{
  #       if(is.null(.y)) {
  #         return(.x)
  #       } else if (is.null(.x)) {
  #         return(.y)
  #       }
  #       right_join(.x, .y, by = by, copy = copy, ...)
  #     }, outdir = outdir)
  #     return(res)
  #   }
  # }
#}
