#' Checks if a folder is a disk.frame
#' @param df a disk.frame or directory to check 
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' is_disk.frame(cars) # FALSE
#' is_disk.frame(cars.df) # TRUE
#' 
#' # clean up cars.df
#' delete(cars.df)
is_disk.frame <- function(df) {
  ##browser
  if("disk.frame" %in% class(df)) {
    df = attr(df, "path", exact=TRUE)
  } else if(!"character" %in% class(df)) { # character then check the path
    return(FALSE)
  }
  
  files <- fs::dir_ls(df, type="file", all  = TRUE)
  # if all files are fst
  if(length(files)>0) {
    if(any(purrr::map_lgl(files, ~length(grep(glob2rx("*.fst"), .x)) == 0))) {
      # some of the fiels do not have a .fst extension
      return(FALSE)
    }
  }
  
  dirs = fs::dir_ls(df, type="directory", all = TRUE)
  if(length(dirs) > 1) {
    return(FALSE)
  } else if(length(dirs) == 1) {
    if(substr(dirs, nchar(dirs)-8,nchar(dirs)) != ".metadata") {
      return(FALSE)
    }
  }
  
  return(TRUE)
}

