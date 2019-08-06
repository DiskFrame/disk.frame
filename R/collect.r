#' Bring the disk.frame into R as data.table/data.frame
#' @param x a disk.frame
#' @param parallel if TRUE the collection is performed in parallel. By default
#'   if there are delayed/lazy steps then it will be parallel, otherwise it will
#'   not be in parallel. This is because parallel requires transferring data
#'   from background R session to the current R session and if there is no
#'   computation then it's better to avoid transferring data between session,
#'   hence parallel = FALSE is a better choice
#' @param ... not used
#' @export
#' @importFrom data.table data.table as.data.table
#' @importFrom furrr future_map_dfr future_options
#' @importFrom purrr map_dfr
#' @importFrom dplyr collect select mutate
#' @rdname collect
#' @examples 
#' cars.df = as.disk.frame(cars)
#' # use collect to bring the data into RAM as a data.table/data.frame
#' collect(cars.df)
#' 
#' # clean up
#' delete(cars.df)
collect.disk.frame <- function(x, ..., parallel = !is.null(attr(x,"lazyfn"))) {
  #cids = get_chunk_ids(x, full.names = T)
  cids = as.integer(get_chunk_ids(x))
  
  if(nchunks(x) > 0) {
    if(parallel) {
      #furrr::future_map_dfr(cids, ~get_chunk(x, .x, full.names = T), .options = furrr::future_options(packages = "disk.frame"))
      #furrr::future_map_dfr(cids, ~disk.frame::get_chunk(x, .x, full.names = T))
      furrr::future_map_dfr(cids, ~get_chunk(x, .x))
      #purrr::map_dfr(cids, ~get_chunk(x, .x, full.names = T))
      #future.apply::future_lapply(chunk_ids, function(.x) disk.frame::get_chunk(x, .x))
      #lapply(chunk_ids, function(chunk) get_chunk(x, chunk)) %>% rbindlist
    } else {
      purrr::map_dfr(cids, ~get_chunk(x, .x, full.names = T))
    }
  } else {
    data.table()
  }
}

#' Bring the disk.frame into R as list
#' @param simplify Should the result be simplified to array
#' @export
#' @rdname collect
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # returns the result as a list
#' collect_list(map(cars.df, ~1))
#' 
#' # clean up
#' delete(cars.df)
collect_list <- function(x, simplify = F, parallel = !is.null(attr(x,"lazyfn"))) {
  if(nchunks(x) > 0) {
    res <- NULL
    if (parallel) {
      #res = furrr::future_map(1:nchunks(x), ~get_chunk(x, .x))
      res = future.apply::future_lapply(1:nchunks(x), function(.x) {
        get_chunk(x, .x)
      })
    } else {
      res = purrr::map(1:nchunks(x), ~get_chunk(x, .x))
    }
    if (simplify) {
      return(simplify2array(res))
    } else {
      return(res)
    }
  } else {
    list()
  }
}
