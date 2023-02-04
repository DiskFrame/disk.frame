#' [[ interface for disk.frame using fst backend
#' @param df a disk.frame
#' @param ... same as data.table
#' @param keep the columns to srckeep
#' @param rbind Whether to rbind the chunks. Defaults to TRUE
#' @param use.names Same as in data.table::rbindlist
#' @param fill Same as in data.table::rbindlist
#' @param idcol Same as in data.table::rbindlist
#' @import fst 
#' @importFrom future.apply future_lapply
#' @importFrom data.table rbindlist 
#' @export
#' @examples 
#' cars.df = as.disk.frame(cars)
#' speed_limit = 50
#' cars.df[[speed < speed_limit ,.N, cut(dist, pretty(dist))]]
#' 
#' # clean up
#' delete(cars.df)
`[.disk.frame` <- function(df, ..., keep = NULL, rbind = TRUE, use.names = TRUE, fill = FALSE, idcol = NULL) {
  message("data.table syntax for disk.frame may be moved to a separate package in the future")

  keep_for_future = keep
  
  code = substitute(chunk[...])
  
  globals_and_pkgs = find_globals_recursively(code, parent.frame())
  
  res = future.apply::future_lapply(get_chunk_ids(df, full.names = TRUE), function(chunk_id) {
  #res = lapply(get_chunk_ids(df, full.names = TRUE), function(chunk_id) {
    chunk = get_chunk(df, chunk_id, full.names=TRUE, keep = keep_for_future)
    data.table::setDT(chunk)
    res = eval(code, envir=globals_and_pkgs$globals)
    res
  }
  , future.packages = c("data.table", globals_and_pkgs$packages),
   future.seed=TRUE
  )

  if(rbind & all(sapply(res, function(x) "data.frame" %in% class(x)))) {
    rbindlist(res, use.names = use.names, fill = fill, idcol = idcol)
  } else if(rbind)  {
    unlist(res)
  } else {
    res
  }
}

#' @export
#' @rdname data.table
`[.disk.frame` <- function(df, ...) {
  message("`df[...] syntax for {disk.frame} has been deprecated. Use `df[[...]]` instead")
  `[[.disk.frame`(df, ...)
}

# Solutions from https://stackoverflow.com/questions/57122960/how-to-use-non-standard-evaluation-nse-to-evaluate-arguments-on-data-table?answertab=active#tab-top
# 
# `[.dd` <- function(x,...) {
#   a <- substitute(...()) #this is an alist
#   expr <- quote(x[[i]])
#   expr <- c(expr, a)
#   res <- list()
#   for (i in seq_along(x)) {
#     res[[i]] <- do.call(`[`, expr)
#   }
#   res
# }