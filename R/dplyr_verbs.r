library(dplyr)
library(dtplyr)

#' @export
#' @import dplyr
select_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(select_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
rename_.disk.frame <- function(.data, ..., .dots){
  # .dots <- lazyeval::all_dots(.dots, ...)
  # cmd <- lazyeval::lazy(rename_(.data, .dots=.dots))
  # record(.data, cmd)
  stop("not implemented rename!")
}

#' @export
filter_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(filter_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
mutate_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(mutate_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
transmute_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(transmute_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
summarise_.disk.frame <- function(.data, ..., .dots){
  .data$.warn <- TRUE
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(summarise_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
do_.disk.frame <- function(.data, ..., .dots){
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(do_(.data, .dots=.dots))
  record(.data, cmd)
}

#' @export
inner_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ...){
  # note that x is named .data in the lazy evaluation
  .data <- x
  cmd <- lazyeval::lazy(inner_join(.data, y, by, copy, ...))
  record(.data, cmd)
}

#' @export
left_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ...){
  # note that x is named .data in the lazy evaluation
  .data <- x
  cmd <- lazyeval::lazy(left_join(.data, y, by, copy, ...))
  record(.data, cmd)
}

#' @export
semi_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ...){
  # note that x is named .data in the lazy evaluation
  .data <- x
  cmd <- lazyeval::lazy(semi_join(.data, y, by, copy, ...))
  record(.data, cmd)
}

#' @export
anti_join.disk.frame <- function(x, y, by=NULL, copy=FALSE, ...){
  # note that x is named .data in the lazy evaluation
  .data <- x
  cmd <- lazyeval::lazy(anti_join(.data, y, by, copy, ...))
  record(.data, cmd)
}

#' @export
groups.disk.frame <- function(x){
  # if (is.null(x$.groups)){
  #   x$.groups <- groups(collect(x, first_chunk_only=TRUE))
  # }
  # x$.groups
  stop("groups.disk.frame not yet implemented")
}

#' @export
group_by_.disk.frame <- function(.data, ..., .dots, add=FALSE){
  .data$.warn <- TRUE
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(group_by_(.data, .dots=.dots, add=add))
  record(.data, cmd)
}

record <- function(.data, cmd){
  attr(.data,"lazyfn") <- c(attr(.data,"lazyfn"), list(cmd))
  .data
}

play <- function(.data, cmds=NULL){
  for (cmd in cmds){
    if (typeof(cmd) == "closure") {
      .data <- cmd(.data)
      print(.data)
    } else {
      .data <- lazyeval::lazy_eval(cmd, list(.data=.data)) 
    }
  }
  .data
}