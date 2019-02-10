#' Set up disk.frame environment
#' @param future_backend which future backend to use for parallelisation
#' @param workers the number of worker (background R processes in the multiprocess environment)
#' @param ... passed to `future::plan`
#' @importFrom future plan multiprocess
#' @export
setup_disk.frame <- function(workers = parallel::detectCores(logical = F), ...) {    
  #requireNamespace("data.table") # this is required for things like data.table
  future::plan(multiprocess, workers = workers, gc = T, ...)
  options(future.globals.maxSize=Inf)
  options(disk.frame.nworkers = workers)
}
