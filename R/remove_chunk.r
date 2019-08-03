#' removes a chunk from the disk.frame
#' @param df a disk.frame
#' @param chunk_id the chunk ID of the chunk to remove. If it's a number then return number.fst
#' @param full.names TRUE or FALSE. Defaults to F. If true then chunk_id is the full path to the chunk otherwise it's the relative path
#' @export
remove_chunk <- function(df, chunk_id, full.names = FALSE) {
  filename = ""
  path = attr(df,"path")
  if(is.numeric(chunk_id)) {
    filename = file.path(path,glue::glue("{as.integer(chunk_id)}.fst"))
  } else {
    if (full.names) {
      filename = chunk_id
    } else {
      filename = file.path(path, chunk_id)
    }
  }
  
  if(filename %in% fs::dir_ls(path, glob="*.fst")) {
    fs::file_delete(filename)
  } else {
    warning("the chunk {filename} does not exists and hence can't be removed; make sure you suffix the file with the .fst extension")
  }
  df
}