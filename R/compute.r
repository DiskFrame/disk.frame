#' Compute without writing
#' @description 
#' Perform the computation; same as calling cmap without .f and lazy = FALSE
#' @param x a disk.frame
#' @param outdir the output directory
#' @param overwrite whether to overwrite or not
#' @param name Not used. Kept for compatibility with dplyr
#' @param ... Not used. Kept for dplyr compatibility
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
compute.disk.frame <- function(x, name, outdir = tempfile("tmp_df_", fileext=".df"), overwrite = TRUE, ...) {
  overwrite_check(outdir, overwrite)
  write_disk.frame(x, outdir = outdir, overwrite = TRUE)
}
