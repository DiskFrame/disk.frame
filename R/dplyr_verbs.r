#' dplyr verbs implemented for disk.frame
#' @export
#' @importFrom dplyr select rename filter mutate transmute arrange do groups group_by group_by glimpse summarise
#' @param ... Same as the dplyr functions
#' @param .data a disk.frame
#' @importFrom lazyeval all_dots
#' @rdname dplyr_verbs
select.disk.frame <- function(.data, ...) {
  quo_dotdotdot = enquos(...)
  map(.data, ~{
    code = quo(select(.x, !!!quo_dotdotdot))
    rlang::eval_tidy(code)
  }, lazy = T)
}
# @param .dots this represents the ... from a higher level
# select_.disk.frame <- function(.data, ..., .dots){
#   .dots <- lazyeval::all_dots(.dots, ...)
#   cmd <- lazyeval::lazy(select_(.data, .dots=.dots))
#   record(.data, cmd)
# }

#' A function to make it easier to create dplyr function for disk.frame
#' @export
create_dplyr_mapper <- function(dplyr_fn) {
  return_func <- function(.data, ...) {
    quo_dotdotdot = enquos(...)
  
    # this is designed to capture any global stuff
    vars_and_pkgs = future::getGlobalsAndPackages(quo_dotdotdot)
    data_for_eval_tidy = force(vars_and_pkgs$globals)
    
    res = map(.data, ~{
      this_env = environment()
      
      if(length(data_for_eval_tidy) > 0) {
        for(i in 1:length(data_for_eval_tidy)) {
          assign(names(data_for_eval_tidy)[i], data_for_eval_tidy[[i]], pos= this_env)
        }
      }
      
      lapply(quo_dotdotdot, function(x) {
        attr(x, ".Environment") = this_env
      })
      
      code = quo(dplyr_fn(.x, !!!quo_dotdotdot))
      eval(parse(text=as_label(code)), envir = this_env)
    }, lazy = T)
  }
  return_func
}


#' @export
#' @rdname dplyr_verbs
rename.disk.frame <- create_dplyr_mapper(rename)

#' @export
#' @rdname dplyr_verbs
filter.disk.frame <- create_dplyr_mapper(filter)

#' mutate
#' @export
#' @rdname dplyr_verbs
#' @importFrom future getGlobalsAndPackages
#' @importFrom rlang eval_tidy quo enquos
mutate.disk.frame <- create_dplyr_mapper(mutate)

#' @export
#' @importFrom dplyr transmute
#' @rdname dplyr_verbs
transmute.disk.frame <- create_dplyr_mapper(transmute)

#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
arrange.disk.frame <- create_dplyr_mapper(arrange)

#' @export
#' @importFrom dplyr summarise
#' @rdname dplyr_verbs
summarise.disk.frame <- create_dplyr_mapper(summarise)

#' @export
#' @importFrom dplyr do
#' @rdname dplyr_verbs
do.disk.frame <- create_dplyr_mapper(do)


#' Group
#' @export
#' @param x a disk.frame
#' @rdname group_by
groups.disk.frame <- function(x){
  shardkey(x)
}

#' Group by designed for disk.frames
#' @importFrom dplyr group_by_
#' @param .data a disk.frame
#' @param ... same as the dplyr::group_by
#' @param add same as dplyr::group_By
#' @param outdir output directory
#' @param overwrite overwrite existing directory
#' @param .dots Previous ... (dots)
#' @export
#' @rdname group_by
#' function(.data, ..., add = FALSE, outdir = NULL, overwrite = T)
group_by.disk.frame <- create_dplyr_mapper(group_by)

#' Take a glimpse
#' @export
#' @rdname dplyr_verbs
glimpse.disk.frame <- function(.data, ...) {
  glimpse(head(.data, ...), ...)
}

# Internal methods
# @param .data the data
# @param cmd the function to record
record <- function(.data, cmd){
  attr(.data,"lazyfn") <- c(attr(.data,"lazyfn"), list(cmd))
  .data
}

# Internal methods
# @param .data the disk.frame
# @param cmds the list of function to play back
# @importFrom lazyeval lazy_eval
play <- function(.data, cmds=NULL){
  #
  for (cmd in cmds){
    if (typeof(cmd) == "closure") {
      .data <- cmd(.data)
      #print(.data)
    } else {
      #.data <- lazyeval::lazy_eval(cmd, list(.data=.data)) 
      # create a temporary environment 
      an_env = new.env(parent = environment())
      
      ng = names(cmd$vars_and_pkgs$globals)
      
      for(i in 1:length(cmd$vars_and_pkgs$globals)) {
        g = cmd$vars_and_pkgs$globals[[i]]
        assign(ng[i], g, pos = an_env)
      }
      
      .data <- do.call(cmd$func, list(.data), envir = an_env)
    }
  }
  .data
}


