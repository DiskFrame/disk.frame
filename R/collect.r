#' Bring the disk.frame into R as data.table/data.frame
#' @param x a disk.frame
#' @param parallel if TRUE the collection is performed in parallel. By default if there are delayed/lazy steps then it will be parallel, otherwise it will not be in parallel. This is because parallel requires transferring data from background R session to the current R session and if there is no computation then it's better to avoid transferring data between session, hence parallel = F is a better choice
#' @param ... not used
#' @export
#' @importFrom data.table data.table as.data.table
#' @importFrom furrr future_map_dfr
#' @importFrom purrr map_dfr
#' @importFrom dplyr collect
#' @rdname collect
collect.disk.frame <- function(x, ..., parallel = !is.null(attr(x,"lazyfn"))) {
  cids = get_chunk_ids(x, full.names = T)
  
  if(nchunks(x) > 0) {
    if(parallel) {
      furrr::future_map_dfr(cids, ~get_chunk(x, .x, full.names = T))
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
collect_list <- function(x, simplify = F, parallel = !is.null(attr(df,"lazyfn"))) {
  df = x
  if(nchunks(df) > 0) {
    res <- NULL
    if (parallel) {
      res = furrr::future_map(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
    } else {
      res = purrr::map(1:nchunks(df), ~get_chunk.disk.frame(df, .x))
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

