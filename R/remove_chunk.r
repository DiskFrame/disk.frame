#' removes a chunk from the disk.frame
#' @param df a disk.frame
#' @param chunk_id the chunk ID of the chunk to remove. If it's a number then return number.fst
#' @param full.names TRUE or FALSE. Defaults to FALSE. If true then chunk_id is the full path to the chunk otherwise it's the relative path
#' @export
#' @examples
#' # TODO add these to tests
#' cars.df = as.disk.frame(cars, nchunks = 4)
#' 
#' # removes 3rd chunk
#' remove_chunk(cars.df, 3)
#' nchunks(cars.df) # 3
#' 
#' # removes 4th chunk
#' remove_chunk(cars.df, "4.fst")
#' nchunks(cars.df) # 3
#'
#' # removes 2nd chunk
#' remove_chunk(cars.df, file.path(attr(cars.df, "path"), "2.fst"), full.names = TRUE)
#' nchunks(cars.df) # 1
#' 
#' # clean up cars.df
#' delete(cars.df)
remove_chunk <- function(df, chunk_id, full.names = FALSE) {
  filename = ""
  path = attr(df,"path")
  if(is.numeric(chunk_id)) {
    filename = file.path(path, glue::glue("{as.integer(chunk_id)}.fst"))
  } else {
    if (full.names) {
      filename = chunk_id
    } else {
      filename = file.path(path, chunk_id)
    }
  }
  
  #if(filename %in% fs::dir_ls(path, glob="*.fst")) {
  if(fs::file_exists(filename)) {
    fs::file_delete(filename)
  } else {
    warning(glue::glue("the chunk {filename} does not exist and hence can't be removed; make sure you suffix the file with the .fst extension"))
  }
  df
}