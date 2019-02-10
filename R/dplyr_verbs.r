#' dplyr version implemented for disk.frame
#' @export
#' @import dplyr
#' @param ... Same as the dplyr functions
#' @param .data disk.frame
#' @param .dots this represents the ...
#' @importFrom lazyeval all_dots
#' @rdname dplyr_verbs
select_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(select_(.data, .dots=.dots))
  record(.data, cmd)
}


#' @export
#' @rdname dplyr_verbs
rename_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(rename_(.data, .dots=.dots))
  record(.data, cmd)
  #stop("not implemented rename!")
}

#' @export
#' @rdname dplyr_verbs
filter_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(filter_(.data, .dots=.dots))
  record(.data, cmd)
}

#' mutate
#' @export
#' @rdname dplyr_verbs
mutate_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(mutate_(.data, .dots=.dots))
  record(.data, cmd)
}

#' transmuate
#' @export
#' @rdname dplyr_verbs
transmute_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(transmute_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
#' @rdname dplyr_verbs
arrange_.disk.frame <- function(.data, ..., .dots){
  warning("disk.frame only sorts (arange) WITHIN each chunk")
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(arrange_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
#' @rdname dplyr_verbs
summarise_.disk.frame <- function(.data, ..., .dots){
  .data$.warn <- TRUE
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(summarise_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
#' @rdname dplyr_verbs
do_.disk.frame <- function(.data, ..., .dots){
  warning("applying `do` to each chunk of disk.frame; this may not work as expected")
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(do_(.data, .dots=.dots))
  record(.data, cmd)
}

#' Group
#' @export
#' @param x a disk.frame
#' @rdname group_by
groups.disk.frame <- function(x){
  shardkey(x)
}

#' Internal method replicating dplyr's compat_as_lazy_dots as it's not exported
#' @param ... passed to rlang::quos
compat_as_lazy_dots = function (...) {
    structure(class = "lazy_dots", purrr::map(rlang::quos(...), compat_as_lazy))
}

#' Internal method replicating dplyr's compat_as_lazy_dots as it's not exported
#' @importFrom rlang get_expr get_env quos
#' @param quo a quote expression
compat_as_lazy = function (quo) {
    structure(class = "lazy", list(expr = rlang::get_expr(quo), env = rlang::get_env(quo)))
}

#' Group by designed for disk.frames
#' @import dplyr purrr
#' @param .data a disk.frame
#' @param ... same as the dplyr::group_by
#' @param add same as dplyr::group_By
#' @param outdir output directory
#' @param overwrite overwrite existing directory
#' @param .dots Previous ... (dots)
#' @export
#' @rdname group_by
group_by.disk.frame <- function(.data, ..., add = FALSE, outdir = NULL, overwrite = T) {
  ##browser
  dots <- compat_as_lazy_dots(...)
  shardby = purrr::map_chr(dots, ~deparse(.x$expr))
  
  shardinfo = shardkey(.data)
  if(!identical(shardinfo[[1]], shardby)) {
    warning(glue::glue(
      "The shardkeys '{shardinfo[[1]]}' are NOT identical to shardby = '{shardby}'. The group_by operation is applied WITHIN each chunk, hence the results may not be as expected. To address this issue, you can rechunk(df, shardby = your_group_keys) which can be computationally expensive. Otherwise, you may use a second stage summary to obtain the desired result."))
  }
  return(dplyr::group_by_(.data, .dots = compat_as_lazy_dots(...), add = add))
}

#' @export
#' @rdname group_by
group_by_.disk.frame <- function(.data, ..., .dots, add=FALSE){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(group_by_(.data, .dots=.dots, add=add))
  record(.data, cmd)
}

#' Take a glimpse
#' @export
#' @rdname dplyr_verbs
glimpse.disk.frame <- function(.data, ...) {
  glimpse(head(.data, ...), ...)
}

#' Internal methods
#' @param .data the data
#' @param cmd the function to record
record <- function(.data, cmd){
  attr(.data,"lazyfn") <- c(attr(.data,"lazyfn"), list(cmd))
  .data
}

#' Internal methods
#' @param .data the disk.frame
#' @param cmds the list of function to play back
#' @importFrom lazyeval lazy_eval
play <- function(.data, cmds=NULL){
  #list.files(
  for (cmd in cmds){
    if (typeof(cmd) == "closure") {
      .data <- cmd(.data)
      #print(.data)
    } else {
      .data <- lazyeval::lazy_eval(cmd, list(.data=.data)) 
    }
  }
  .data
}