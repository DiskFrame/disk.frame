#' Force computations. The results are stored in a folder.
#' @description 
#' Perform the computation; same as calling cmap without .f and lazy = FALSE
#' @param x a disk.frame
#' @param outdir the output directory
#' @param name If not NULL then used as outdir prefix.
#' @param ... Passed to `write_disk.frame`
#' @export
#' @importFrom dplyr compute
#' @examples
#' cars.df = as.disk.frame(cars)
#' cars.df2 = cars.df %>% cmap(~.x)
#' # the computation is performed and the data is now stored elsewhere
#' cars.df3 = compute(cars.df2)
#' 
#' # clean up
#' delete(cars.df)
#' delete(cars.df3)
compute.disk.frame <- function(x, name = NULL, outdir = tempfile("tmp_df_", fileext=".df"), ...) {
  if (!is.null(name)) {
    warning("in `compute.disk.frame()` name is not NULL, using `name` file name prefix in temporary `outdir` ")
    outdir = tempfile(name, fileext=".df")
  }
  
  write_disk.frame(x, outdir = outdir, ...)
}
