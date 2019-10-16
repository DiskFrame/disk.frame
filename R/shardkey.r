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

#' Compare two disk.frame shardkeys
#' 
#' @param sk1 shardkey1
#' @param sk2 shardkey2
#' @export
shardkey_equal <- function(sk1, sk2) {
  if (sk1$shardkey == "") {
    # if the shardkey is not set then it's the same as having no shardkey
    return(FALSE)
  }
  (sk1$shardkey == sk2$skardkey) && (sk1$shardchunks == sk2$shardchunks)
}