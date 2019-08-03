#' Returns the shardkey (not implemented yet)
#' @importFrom jsonlite fromJSON
#' @param df a disk.frame
#' @export
# TODO make this work
shardkey <- function(df) {
  meta_file = file.path(attr(df,"path"),".metadata", "meta.json")
  if(!file.exists(meta_file)) {
    add_meta(df)
  }
  meta = jsonlite::fromJSON(meta_file)
  list(shardkey = meta$shardkey, shardchunks = meta$shardchunks)
}

