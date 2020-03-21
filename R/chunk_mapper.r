#' Create function that applies to each chunk if disk.frame
#' 
#' A function to make it easier to create functions like \code{filter}
#' 
#' @examples 
#' 
#' filter = create_chunk_mapper(dplyr::filter)
#' 
#' #' example: creating a function that keeps only the first and last n row
#' first_and_last <- function(chunk, n, ...) {
#'   nr = nrow(chunk)
#'   print(nr-n+1:nr)
#'   chunk[c(1:n, (nr-n+1):nr), ]
#' }
#' 
#' #' create the function for use with disk.frame
#' first_and_last_df = create_chunk_mapper(first_and_last)
#' 
#' mtcars.df = as.disk.frame(mtcars)
#' 
#' #' the operation is lazy
#' lazy_mtcars.df = mtcars.df %>%
#'   first_and_last_df(2)
#' 
#' #' bring into R
#' collect(lazy_mtcars.df)
#' 
#' #' clean up
#' delete(mtcars.df)
#' 
#' @param chunk_fn The dplyr function to create a mapper for
#' @param warning_msg The warning message to display when invoking the mapper
#' @param as.data.frame force the input chunk of a data.frame; needed for dtplyr
#' @importFrom rlang enquos quo
#' @export
create_chunk_mapper <- function(chunk_fn, warning_msg = NULL, as.data.frame = TRUE) {
  return_func <- function(.data, ...) {
    if (!is.null(warning_msg)) {
      warning(warning_msg)
    }
    
    browser()
    quo_dotdotdot = rlang::enquos(...)
    
    # this is designed to capture any global stuff
    vars_and_pkgs = future::getGlobalsAndPackages(quo_dotdotdot)
    data_for_eval_tidy = force(vars_and_pkgs$globals)
    
    res = cmap(.data, ~{
      this_env = environment()
      
      if(length(data_for_eval_tidy) > 0) {
        for(i in 1:length(data_for_eval_tidy)) {
          assign(names(data_for_eval_tidy)[i], data_for_eval_tidy[[i]], pos = this_env)
        }
      }
      
      lapply(quo_dotdotdot, function(x) {
        attr(x, ".Environment") = this_env
      })
      
      if(as.data.frame) {
        if("grouped_df" %in% class(.x)) {
          code = rlang::quo(chunk_fn(.x, !!!quo_dotdotdot))
        } else {
          code = rlang::quo(chunk_fn(as.data.frame(.x), !!!quo_dotdotdot))
        }
      } else {
        code = rlang::quo(chunk_fn(.x, !!!quo_dotdotdot))
      }
      
      # ZJ: we need both approaches. TRUST ME
      # TODO better NSE at some point need dist
      tryCatch({
        return(rlang::eval_tidy(code))
      }, error = function(e) {
        as_label_code = rlang::as_label(code)
        if(as_label_code == "chunk_fn(...)") {
          stop(glue::glue("disk.frame has detected a syntax error in \n\n`{code}`\n\n. If you believe your syntax is correct, raise an issue at https://github.com/xiaodaigh/disk.frame with a MWE"))
        } else {
          # likely to be dealing with data.tables
          return(eval(parse(text=as_label_code), envir = this_env))
        }
      })
    }, lazy = TRUE)
  }
  return_func
}
