#' Helper function to evalparse some glue::glue string
#' @export
evalparseglue <- function(code, env = parent.frame()) {
  eval(parse(text = glue::glue(code, .envir = env)), envir = env)
}
