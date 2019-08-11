#' Perform the computation; same as calling map without .f and lazy = FALSE
#' @param x a disk.frame
#' @param name not used kept for compatibility with dplyr
#' @export
#' @importFrom dplyr compute
#' @rdname map
#' @examples 
#' cars.df = as.disk.frame(cars)
#' cars.df2 = cars.df %>% map(~.x)
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
