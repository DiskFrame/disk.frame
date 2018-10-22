#' Bring the disk.frame into R
#' @export
#' 
function collect.disk.frame(df, ...) {
  map_dfr(1:nchunk(df), ~get_chunk.disk.frame(df, .x)
}

function collect(...) {
  UseMethod("collect")
}