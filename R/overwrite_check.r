#' Check if the outdir exists or not
#' @param outdir the output directory
#' @param overwrite TRUE or FALSE if `outdir`` exists and overwrite = F then throw an error
#' @import fs glue
#' @export
overwrite_check <- function(outdir, overwrite) {
  if (is.null(outdir)) {
    warning("outdir is NULL; no overwrite check is performed")
    return(NULL)
  }
  
  if(overwrite & fs::dir_exists(outdir)) {
    if(!is_disk.frame(outdir)) {
      stop(glue::glue("The directory is {outdir} is not a disk.frame folder. Execution has stopped to prevent accidental deletion of potentially important files"))
    }
    if(length(dir_ls(outdir, all = T)) != 0) {
      tryCatch({
        fs::dir_delete(outdir)
      }, error = function(e) {
        print(e)
        stop("Failed to delete the directory {outdir} in preparation for overwrite, this could be due to many reason and may be a geniune bug. Firstly, though, please ensure you do not have the folder open by Explorer (Windows) or other file management systems")
      })
    }
    
    fs::dir_create(outdir)
  } else if(overwrite == F & fs::dir_exists(outdir)) {
    stop(glue::glue("overwrite  = F and outdir '{outdir}' already exists"))
  } else {
    fs::dir_create(outdir)
  }
}