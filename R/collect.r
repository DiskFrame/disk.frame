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
collect.disk.frame <- function(x, ..., parallel = !is.null(attr(x,"recordings"))) {
  cids = get_chunk_ids(x, full.names = T, strip_extension = F)
  
  if (parallel) {
    list_of_data.table = future.apply::future_lapply(cids, function(cid) {
      get_chunk(x, cid, full.names = TRUE)
    })
  } else {
    list_of_data.table = lapply(cids, function(cid) {
      get_chunk(x, cid, full.names = TRUE)
    })
  }
  
  data.table::rbindlist(list_of_data.table)
}

#' @param simplify Should the result be simplified to array
#' @export
#' @rdname collect
#' @return collect_list returns a list
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # returns the result as a list
#' collect_list(cmap(cars.df, ~1))
#' 
#' # clean up
#' delete(cars.df)
collect_list <- function(x, simplify = FALSE, parallel = !is.null(attr(x,"lazyfn"))) {
  error("do it")
  # cids = get_chunk_ids(x, full.names = TRUE, strip_extension = FALSE)
  # 
  # 
  # if(nchunks(x) > 0) {
  #   res <- NULL
  #   if (parallel) {
  #     res = future.apply::future_lapply(cids, function(.x) {
  #       get_chunk(x, .x, full.names = TRUE)
  #     })
  #   } else {
  #     res = purrr::map(cids, ~get_chunk(x, .x, full.names = TRUE))
  #   }
  #   if (simplify) {
  #     return(simplify2array(res))
  #   } else {
  #     return(res)
  #   }
  # } else {
  #   list()
  # }
}
