#' A two-stage aggregation framework
#'
#' @param chunkfn a function to apply to each chunk. The results are passed to a central location for aggregation
#' @param agg The aggregation function in the central location each chunk
#' @param finalize A function to run at the end
#'
#' @return a disk.frame.agg
#' @export
#'
#' @examples
#' 
#' # defining the mean aggregation
#' 
#' chunkfn <- function(chunk) {
#' }
# learning from https://docs.dask.org/en/latest/dataframe-groupby.html
group_by_aggregation <- function(chunkfn, agg, finalize) {
  res = list(chunkfn, agg, finalize)
  class(res) <- "disk.frame.agg"
  res
}

library(disk.frame)
a = disk.frame::as.disk.frame(mtcars)

meanreduce = function(df, by ) {
  df %>% 
    map(~{
      .x %>% 
        group_by()
    })
}
