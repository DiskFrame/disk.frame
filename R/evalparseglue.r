#' Helper function to evalparse some glue::glue string
#' @param code the code in character(string) format to evaluate
#' @param env the environment in which to evaluate the code
#' @export
evalparseglue <- function(code, env = parent.frame()) {
  eval(parse(text = glue::glue(code, .envir = env)), envir = env)
}
