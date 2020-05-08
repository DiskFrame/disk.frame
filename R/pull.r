#' Pull a column from table similiar to `dplyr::pull`. 
#'
#' @export
#' @importFrom dplyr pull
#' @param .data The disk.frame
#' @param var can be an positive or negative integer or a character/string. See dplyr::pull documentation
pull.disk.frame <- function(.data, var = -1) {
  name = as.character(substitute(var))
  name_num = as.numeric(name)
  
  if (!is.na(name_num)) {
    if(name_num > 0) {
      name = names(.data)[name_num]
    } else if (name_num < 0) {
      nd = names(.data)
      name = nd[length(nd) - name_num + 1]
    } else {
      stop(glue::glue("`var` must an integer between 1 and {ncol(.data)} cannot be {var}"))
    }
  }
  
  .data %>% 
    select(!!!syms(name)) %>% 
    collect %>% 
    pull()
}