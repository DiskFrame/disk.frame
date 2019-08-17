#' Check if the outdir exists or not
#' @description 
#' If the overwrite is TRUE then the folder will be deleted, otherwise the folder will be created.
#' @param outdir the output directory
#' @param overwrite TRUE or FALSE if `outdir`` exists and overwrite = FALSE then throw an error
#' @import fs
#' @importFrom glue glue
#' @export
#' 
#' @examples
#' tf = tempfile()
#' overwrite_check(tf, overwrite = FALSE)
#' overwrite_check(tf, overwrite = TRUE)
#' 
#' # clean up
#' fs::dir_delete(tf)
overwrite_check <- function(outdir, overwrite) {
  ##browser
  if (is.null(outdir)) {
    warning("outdir is NULL; no overwrite check is performed")
    return(NULL)
  }
  
  if(overwrite & fs::dir_exists(outdir)) {
    if(!is_disk.frame(outdir)) {
      stop(glue::glue("The directory is {outdir} is not a disk.frame folder. Execution has stopped to prevent accidental deletion of potentially important files"))
    }
    if(length(fs::dir_ls(outdir, all = TRUE)) != 0) {
      tryCatch({
        fs::dir_delete(outdir)
      }, error = function(e) {
        message(e)
        stop(glue::glue("Failed to delete the directory {outdir} in preparation for overwrite, this could be due to many reason and may be a genuine bug. Firstly, though, please ensure you do not have the folder open by Explorer (Windows) or other file management systems"))
      })
    }
    
    fs::dir_create(outdir)
  } else if(overwrite == FALSE & fs::dir_exists(outdir)) {
    stop(glue::glue("overwrite  = FALSE and outdir '{outdir}' already exists"))
  } else {
    fs::dir_create(outdir)
  }
}