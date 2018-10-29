#' Returns the shardkey (not implemented yet)
#' TODO make this work
#' @import jsonlite
#' @export
shardkey <- function(df, ...) {
  meta_file = file.path(attr(df,"path"),".metadata", "meta.json")
  if(!file.exists(meta_file)) {
    add_meta(df)
  }
  meta = jsonlite::fromJSON(meta_file)
  list(shardkey = meta$shardkey, shardchunks = meta$shardchunks)
}
