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
create_chunk_mapper <- function(chunk_fn, warning_msg = NULL, as.data.frame = FALSE) {
  if(as.data.frame) {
    warning("`as.data.frame` is deprecated in create_chunk_mapper")
  } 
  
  return(function(.data, ...) {
    if(!is.null(warning_msg)) {
      print(warning_msg)
    }
    
    
    # you need to use list otherwise the names will be gone
    sub_dotdotdot = substitute(list(...))
    
    code = paste0(deparse(sub_dotdotdot), collapse = "")
    # code will be in the form of "list(...)"
    code = substr(code, 6, nchar(code)-1)
    
    if (code == "NULL") {
      globals_and_pkgs = future::getGlobalsAndPackages(str2lang("chunk_fn()"))
    } else {
      globals_and_pkgs = future::getGlobalsAndPackages(str2lang(sprintf("chunk_fn(%s)", code)))
    }
    
    
    global_vars = globals_and_pkgs$globals
    
    env = parent.frame()
    
    done = identical(env, globalenv())
    
    # keep adding global variables
    
    while(!done) {
      tmp_globals_and_pkgs = future::getGlobalsAndPackages(sub_dotdotdot, envir = env)
      new_global_vars = tmp_globals_and_pkgs$globals
      for (name in setdiff(names(new_global_vars), names(global_vars))) {
        global_vars[name] = new_global_vars[[name]]
      }
      
      done = identical(env, globalenv())
      env = parent.env(env)
    }
    
    globals_and_pkgs$globals = global_vars
    
    attr(.data, "recordings") = c(attr(.data, "recordings"), list(globals_and_pkgs))
    
    .data
  })
}
