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
  message("data.table syntax for disk.frame may be moved to a separate package in the future")

  keep_for_future = keep
  
  code = substitute(chunk[...])
  
  # sometimes the arguments could be empty
  # in a recent version of globals that would cause a fail
  # to avoid the fail remove them from the test
  #dotdotdot_for_find_global = dotdotdot[!sapply(sapply(dotdotdot, as.character), function(x) all(unlist(x) == ""))]
  
  #ag = globals::findGlobals(dotdotdot_for_find_global)
  #ag = setdiff(ag, "") # "" can cause issues with future # this line no longer needed
  
  
  # you need to use list otherwise the names will be gone
  if (paste0(deparse(code), collapse="") == "chunk_fn(NULL)") {
    globals_and_pkgs = future::getGlobalsAndPackages(expression(chunk_fn()))
  } else {
    globals_and_pkgs = future::getGlobalsAndPackages(code)
  }
  
  
  global_vars = globals_and_pkgs$globals
  
  env = parent.frame()
  
  done = identical(env, emptyenv()) || identical(env, globalenv())
  
  # keep adding global variables by moving up the environment chain
  while(!done) {
    tmp_globals_and_pkgs = future::getGlobalsAndPackages(code, envir = env)
    new_global_vars = tmp_globals_and_pkgs$globals
    for (name in setdiff(names(new_global_vars), names(global_vars))) {
      global_vars[[name]] <- new_global_vars[[name]]
    }
    
    done = identical(env, emptyenv()) || identical(env, globalenv())
    env = parent.env(env)
  }
  
  globals_and_pkgs$globals = global_vars
  
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