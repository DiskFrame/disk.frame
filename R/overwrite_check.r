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
    fs::dir_delete(outdir)
    fs::dir_create(outdir)
  } else if(overwrite == F & fs::dir_exists(outdir)) {
    stop(glue::glue("overwrite  = F and outdir '{outdir}' already exists"))
  } else {
    fs::dir_create(outdir)
  }
}