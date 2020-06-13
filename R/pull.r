#' Pull a column from table similar to `dplyr::pull`. 
#'
#' @export
#' @importFrom dplyr pull
#' @param .data The disk.frame
#' @param var can be an positive or negative integer or a character/string. See dplyr::pull documentation
pull.disk.frame <- function(.data, var = -1, name = NULL, ...) {
  ####  get the right name for var
  var_name = paste0(as.character(substitute(var)), collapse = "")
  suppressWarnings(var_name_num <- as.numeric(var_name))
  
  if(!is.na(var_name_num)) {
    if(var_name_num > 0) {
      var_name = names(.data)[var_name_num]
    } else if (var_name_num < 0) {
      nd = names(.data)
      var_name = nd[length(nd) + var_name_num + 1]
    } else {
      stop(glue::glue("`var` must an integer between 1 and {ncol(.data)} cannot be {var}"))
    }
  }
  
  ####  get the right name for name
  name_var = paste0(as.character(substitute(name)), collapse = "")
  suppressWarnings(name_var_num <- as.numeric(name_var))
  
  if(!is.na(name_var_num)) {
    if(name_var_num > 0) {
      name_var = names(.data)[name_var_num]
    } else if (name_var_num < 0) {
      nd = names(.data)
      name_var = nd[length(nd) + name_var_num + 1]
    } else {
      stop(glue::glue("`var` must an integer between 1 and {ncol(.data)} cannot be {var}"))
    }
  }
    
  tmp = .data %>% 
    select(!!!syms(var_name)) %>% 
    collect
  
  if (name_var == "") {
    # if name is null but can't check for that directly due to NSE
    pull(tmp)
  } else {
    code = glue::glue("tmp %>% pull({var_name}, {name_var})")
    eval(parse(text = code))
  }
}
