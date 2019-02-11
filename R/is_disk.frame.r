#' Checks if a folder is a disk.frame
#' @param df a disk.frame or directory to check 
#' @export
is_disk.frame <- function(df) {
  ##browser
  if("disk.frame" %in% class(df)) {
    df = attr(df, "path")
  } else if(!"character" %in% class(df)) { # character then check the path
    return(F)
  }
  
  files <- fs::dir_ls(df, type="file", all  = TRUE)
  # if all files are fst
  if(length(files)>0) {
    if(any(purrr::map_lgl(files, ~length(grep(glob2rx("*.fst"), .x)) == 0))) {
      # some of the fiels do not have a .fst extension
      return(F)
    }
  }
  
  dirs = fs::dir_ls(df, type="directory", all = TRUE)
  if(length(dirs) > 1) {
    return(F)
  } else if(length(dirs) == 1) {
    if(substr(dirs, nchar(dirs)-8,nchar(dirs)) != ".metadata") {
      return(F)
    }
  }
  
  return(T)
}

