#' Set up disk.frame environment
#' @param future_backend which future backend to use for parallelisation
#' @param works
#' @importFrom future plan
#' @export
setup_disk.frame <- function(future_backend = multiprocess, workers = parallel::detectCores(logical = F), ...) {
  library(future)
  future::plan(multiprocess, workers = workers, ...)
}