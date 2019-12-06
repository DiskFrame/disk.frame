#' Bring the disk.frame into R
#'
#' Bring the disk.frame into RAM by loading the data and running all lazy
#' operations as data.table/data.frame or as a list
#' @param x a disk.frame
#' @param parallel if TRUE the collection is performed in parallel. By default
#'   if there are delayed/lazy steps then it will be parallel, otherwise it will
#'   not be in parallel. This is because parallel requires transferring data
#'   from background R session to the current R session and if there is no
#'   computation then it's better to avoid transferring data between session,
#'   hence parallel = FALSE is a better choice
#' @param ... not used
#' @importFrom data.table data.table as.data.table
#' @importFrom furrr future_map_dfr future_options
#' @importFrom purrr map_dfr
#' @importFrom dplyr collect select mutate
#' @return collect return a data.frame/data.table
#' @examples
#' cars.df = as.disk.frame(cars)
#' # use collect to bring the data into RAM as a data.table/data.frame
#' collect(cars.df)
#'
#' # clean up
#' delete(cars.df)
#' @export
#' @rdname collect
collect.summarized_disk.frame <- function(x, ..., parallel = !is.null(attr(x,"lazyfn"))) {
  
  code_to_run = glue::glue("x %>% {attr(x, 'summarize_code') %>% as.character}")
  class(x) <- "disk.frame"
  eval(parse(text = code_to_run))
}
