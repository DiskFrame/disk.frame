#' Move or copy a disk.frame to another location
#'
#' @param df The disk.frame
#' @param outdir The new location
#' @param copy Merely copy and not move
#' @param ... NOT USED
#'
#' @return a disk.frame
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' cars_copy.df = copy_df_to(cars.df, outdir = tempfile(fileext=".df"))
#' 
#' cars2.df = move_to(cars.df, outdir = tempfile(fileext=".df"))
#' 
#' # clean up
#' delete(cars_copy.df)
#' delete(cars2.df)
move_to <- function(df, outdir, ..., copy = FALSE) {
  if(!fs::dir_exists(outdir)) {
    fs::dir_create(outdir)
  }
  
  if(!copy %in% c(TRUE, FALSE)) {
    stop("disk.frame::move_to ERROR: copy argument must be TRUE or FALSE")
  }
  
  ## copy all files over
  listfiles = list.files(attr(df,"path", exact=TRUE), full.names = TRUE)
  shortlistfiles = list.files(attr(df,"path", exact=TRUE))
  purrr::walk2(listfiles, shortlistfiles, ~{
    if(copy) {
      fs::file_copy(.x, file.path(outdir, .y))
    } else {
      fs::file_move(.x, file.path(outdir, .y))
    }
  })

  ## copy .metadata over
  fs::dir_create(file.path(outdir, ".metadata"))
  
  metadata_path = file.path(attr(df,"path", exact=TRUE), ".metadata")
  
  listfiles = list.files(metadata_path, full.names = TRUE)
  shortlistfiles = list.files(metadata_path)
  purrr::walk2(listfiles, shortlistfiles, ~{
    if(copy) {
      fs::file_copy(.x, file.path(outdir, ".metadata", .y))
    } else {
      fs::file_move(.x, file.path(outdir, ".metadata", .y))
    }
  })
  
  if(!copy) {
    delete(df)
  }
  
  disk.frame(outdir)
}

#' @rdname move_to
#' @export
copy_df_to <- function(df, outdir, ...) {
  move_to(df, outdir, ..., copy = TRUE)
}