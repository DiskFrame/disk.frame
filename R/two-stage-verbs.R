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
group_by_aggregation <- function(chunk, agg, finalize) {
  res = list(chunkfn, agg, finalize)
  class(res) <- "disk.frame.agg"
  res
}

library(disk.frame)
a = disk.frame::as.disk.frame(mtcars)


chunkmean = function(x) {
  sum(x)
}

aggmean = function(x) {
  sum(x)
}

finalizemean = function(x) {
  mutate(x$numerator, x$denominator)
}

group_by.disk.frame <- function(df, by, ...) {
  df %>% 
    chunk_group_by({{by}}) %>% 
    chunk_summarise(tmp1 = chunkmean(var)) %>% 
    collect() %>% 
    group_by({{by}}) %>% 
    summarise(aggmean(var))
    
}

a %>% 
  chunk_group_by(gear) %>%
  chunk_summarise(mean(mpg)) %>% 
  collect

groupreduce = function(df, by, vars) {
  vars1 = purrr::map(vars, ~{
    glue("`_sum{.x}` = sum(.x)")
  }) %>% 
    paste("tmp123456789 = n()", collapse = ", ")
  
  # TODO fix this
  df %>% 
    group_by({{by}}) %>% 
    summarise() %>% 
    collect %>% 
    group_by({{by}}) %>% 
    summarise()
}
