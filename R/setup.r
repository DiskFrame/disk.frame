#' Set up disk.frame environment
#' @param future_backend which future backend to use for parallelization
#' @param workers the number of workers (background R processes in the multiprocess environment)
#' @param ... passed to `future::plan`
#' @importFrom future plan multiprocess
#' @export
setup_disk.frame <- function(future_backend = multiprocess, workers = parallel::detectCores(logical = FALSE), ...) {    
  future::plan(future_backend, workers = workers, gc = TRUE, ...)
  options(future.globals.maxSize=Inf) # do not limit the amount of transfers to other workers
  options(disk.frame.nworkers = workers)
}
