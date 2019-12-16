#' @rdname cmap2
#' @export
map_by_chunk_id <- function(.x, .y, .f, ..., outdir) {
  warning("map_by_chunk_id is deprecated. Use map2 instead")
  map2.disk.frame(.x, .y, .f, ..., outdir = outdir)
}