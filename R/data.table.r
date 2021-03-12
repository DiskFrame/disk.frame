#' [ interface for disk.frame using fst backend
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
#' cars.df[speed < speed_limit ,.N, cut(dist, pretty(dist))]
#' 
#' # clean up
#' delete(cars.df)
`[.disk.frame` <- function(df, ..., keep = NULL, rbind = TRUE, use.names = TRUE, fill = FALSE, idcol = NULL) {

  keep_for_future = keep
  
  dotdotdot = substitute(...()) #this is an alist
  
  # sometimes the arguments could be empty
  # in a recent version of globals that would cause a fail
  # to avoid the fail remove them from the test
  dotdotdot_for_find_global = dotdotdot[!sapply(sapply(dotdotdot, as.character), function(x) all(unlist(x) == ""))]
  
  ag = globals::findGlobals(dotdotdot_for_find_global)
  #ag = setdiff(ag, "") # "" can cause issues with future # this line no longer needed
  
  res = future.apply::future_lapply(get_chunk_ids(df, strip_extension = FALSE), function(chunk_id) {
  #lapply(get_chunk_ids(df, strip_extension = FALSE), function(chunk_id) {
    chunk = get_chunk(df, chunk_id, keep = keep_for_future)
    data.table::setDT(chunk)
    expr <- quote(chunk)
    expr <- c(expr, dotdotdot)
    res <- do.call(`[`, expr)
    res
  }, future.globals = c("df", "keep_for_future", "dotdotdot", ag), future.packages = c("data.table","disk.frame"),
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