#' dplyr verbs implemented for disk.frame
#' @export
#' @importFrom dplyr select rename filter mutate transmute arrange do groups group_by group_by glimpse summarise
#' @param ... Same as the dplyr functions
#' @param .data a disk.frame
#' @rdname dplyr_verbs
select.disk.frame <- function(.data, ...) {
  quo_dotdotdot = rlang::enquos(...)
  map(.data, ~{
    code = rlang::quo(select(.x, !!!quo_dotdotdot))
    rlang::eval_tidy(code)
  }, lazy = T)
}

#' A function to make it easier to create dplyr function for disk.frame
#' @param dplyr_fn The dplyr function to create a mapper for
#' @param warning_msg The warning message to display when invoking the mapper
#' @importFrom rlang enquos quo
#' @export
create_dplyr_mapper <- function(dplyr_fn, warning_msg = NULL) {
  return_func <- function(.data, ...) {
    if (!is.null(warning_msg)) {
      warning(warning_msg)
    }
    
    quo_dotdotdot = rlang::enquos(...)
  
    # this is designed to capture any global stuff
    vars_and_pkgs = future::getGlobalsAndPackages(quo_dotdotdot)
    data_for_eval_tidy = force(vars_and_pkgs$globals)
    
    res = map(.data, ~{
      this_env = environment()
      
      if(length(data_for_eval_tidy) > 0) {
        for(i in 1:length(data_for_eval_tidy)) {
          assign(names(data_for_eval_tidy)[i], data_for_eval_tidy[[i]], pos = this_env)
        }
      }
      
      lapply(quo_dotdotdot, function(x) {
        attr(x, ".Environment") = this_env
      })
      
      code = rlang::quo(dplyr_fn(.x, !!!quo_dotdotdot))
      eval(parse(text=rlang::as_label(code)), envir = this_env)
    }, lazy = T)
  }
  return_func
}


#' @export
#' @rdname dplyr_verbs
rename.disk.frame <- create_dplyr_mapper(dplyr::rename)

#' @export
#' @rdname dplyr_verbs
filter.disk.frame <- create_dplyr_mapper(dplyr::filter)

#' mutate
#' @export
#' @rdname dplyr_verbs
#' @importFrom future getGlobalsAndPackages
#' @importFrom rlang eval_tidy quo enquos
#' @importFrom dplyr mutate
mutate.disk.frame <- create_dplyr_mapper(dplyr::mutate)

#' @export
#' @importFrom dplyr transmute
#' @rdname dplyr_verbs
transmute.disk.frame <- create_dplyr_mapper(dplyr::transmute)

#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
arrange.disk.frame <- create_dplyr_mapper(dplyr::arrange, "disk.frame only sorts (arange) WITHIN each chunk")

#' @export
#' @importFrom dplyr summarise
#' @rdname dplyr_verbs
summarise.disk.frame <- create_dplyr_mapper(dplyr::summarise)

#' @export
#' @importFrom dplyr summarize
#' @rdname dplyr_verbs
summarize.disk.frame <- create_dplyr_mapper(dplyr::summarize)

#' Group
#' @export
#' @param x a disk.frame
groups.disk.frame <- function(x){
  shardkey(x)
}

#' Group by designed for disk.frames
#' @importFrom dplyr group_by_
#' @param .data a disk.frame
#' @param ... same as the dplyr::group_by
#' @export
#' @rdname group_by
# TODO check shardkey
group_by.disk.frame <- function(.data, ...) {
  dplyr_fn = dplyr::group_by
  
  quo_dotdotdot = rlang::enquos(...)
  
  # this is designed to capture any global stuff
  vars_and_pkgs = future::getGlobalsAndPackages(quo_dotdotdot)
  data_for_eval_tidy = force(vars_and_pkgs$globals)
  
  res = map(.data, ~{
    this_env = environment()
    
    if(length(data_for_eval_tidy) > 0) {
      for(i in 1:length(data_for_eval_tidy)) {
        assign(names(data_for_eval_tidy)[i], data_for_eval_tidy[[i]], pos = this_env)
      }
    }
    
    lapply(quo_dotdotdot, function(x) {
      attr(x, ".Environment") = this_env
    })
    
    code = rlang::quo(dplyr_fn(.x, !!!quo_dotdotdot))
    eval(parse(text=rlang::as_label(code)), envir = this_env)
  }, lazy = T)
}
#group_by.disk.frame <- create_dplyr_mapper(dplyr::group_by, "The group_by operation is applied WITHIN each chunk, hence the results may not be as expected. To address this issue, you can rechunk(df, shardby = your_group_keys) which can be computationally expensive. Otherwise, you may use a second stage summary to obtain the desired result.")

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
play <- function(.data, cmds=NULL){
  for (cmd in cmds){
    if (typeof(cmd) == "closure") {
      .data <- cmd(.data)
    } else {
      # create a temporary environment 
      an_env = new.env(parent = environment())
      
      ng = names(cmd$vars_and_pkgs$globals)
      
      if(length(ng) > 0) {
        for(i in 1:length(cmd$vars_and_pkgs$globals)) {
          g = cmd$vars_and_pkgs$globals[[i]]
          assign(ng[i], g, pos = an_env)
        }
      }
      .data <- do.call(cmd$func, list(.data), envir = an_env)
    }
  }
  .data
}


