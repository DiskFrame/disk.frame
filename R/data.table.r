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
#' @importFrom globals findGlobals
#' @export
#' @examples 
#' cars.df = as.disk.frame(cars)
#' speed_limit = 50
#' cars.df[[speed < speed_limit ,.N, cut(dist, pretty(dist))]]
#' 
#' # clean up
#' delete(cars.df)
#' @rdname data.table
`[[.disk.frame` <- function(df, ..., keep = NULL, rbind = TRUE, use.names = TRUE, fill = FALSE, idcol = NULL) {
  browser()
  # keep_for_future = keep
  # 
  # dotdotdot = substitute(...()) #this is an alist
  # 
  # ag = globals::findGlobals(dotdotdot)
  # ag = setdiff(ag, "") # "" can cause issues with future
  
  cids = get_chunk_ids(df, strip_extension = FALSE)
  library(future)
  res = future.apply::future_lapply(cids, function(chunk_id) {
    return(1)
  #lapply(get_chunk_ids(df, strip_extension = FALSE), function(chunk_id) {
    # chunk = get_chunk(df, chunk_id, keep = keep_for_future)
    # data.table::setDT(chunk)
    # expr <- quote(chunk)
    # 
    # expr <- c(expr, dotdotdot)
    # return(expr)s
    #res <- do.call(`[.data.table`, expr)
    #res
  #}, future.globals = c("df", "keep_for_future", "dotdotdot", ag), future.packages = c("data.table","disk.frame"))
  })
  
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
# `[.dd` <- function(x, ...) {
#   code <- rlang::enexprs(...)
#   lapply(x, function(dt) {
#     ex <- rlang::expr(dt[!!!code])
#     rlang::eval_tidy(ex)
#   })
# }
# 
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