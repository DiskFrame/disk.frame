#' Checks if a folder is a disk.frame
#' @param dir a disk.frame or directory to check 
#' @export
is_disk.frame <- function(dir) {
  if("disk.frame" %in% class(dir)) {
    dir = attr(dir, "path")
  }
  
  files <- fs::dir_ls(dir, type="file", all  = TRUE)
  # if all files are fst
  if(length(files)>0) {
    if(any(purrr::map_lgl(files, ~length(grep(glob2rx("*.fst"), .x)) == 0))) {
      # some of the fiels do not have a .fst extension
      return(F)
    }
  }
  
  dirs = fs::dir_ls(dir, type="directory", all = TRUE)
  if(length(dirs) > 1) {
    return(F)
  } else if(length(dirs) == 1) {
    if(dirs != file.path(dir, ".metadata")) {
      return(F)
    }
  }
  
  return(T)
}

