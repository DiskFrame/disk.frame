#' Create dplyr function for disk.frame
#' 
#' A function to make it easier to create functions like \code{filter}
#' 
#' @examples 
#' 
#' filter = create_dplyr_mapper(dplyr::filter)
#' 
#' #' example: creating a function that keeps only the first and last n row
#' first_and_last <- function(chunk, n, ...) {
#'   nr = nrow(chunk)
#'   print(nr-n+1:nr)
#'   chunk[c(1:n, (nr-n+1):nr), ]
#' }
#' 
#' #' create the function for use with disk.frame
#' first_and_last_df = create_dplyr_mapper(first_and_last)
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
#' @param dplyr_fn The dplyr function to create a mapper for
#' @param warning_msg The warning message to display when invoking the mapper
#' @param as.data.frame force the input chunk of a data.frame; needed for dtplyr
#' @importFrom rlang enquos quo
#' @export
create_dplyr_mapper <- function(dplyr_fn, warning_msg = NULL, as.data.frame = TRUE) {
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
      
      if(as.data.frame) {
        if("grouped_df" %in% class(.x)) {
          code = rlang::quo(dplyr_fn(.x, !!!quo_dotdotdot))
        } else {
          code = rlang::quo(dplyr_fn(as.data.frame(.x), !!!quo_dotdotdot))
        }
      } else {
        code = rlang::quo(dplyr_fn(.x, !!!quo_dotdotdot))
      }
      
      # ZJ: we need both approaches. TRUST ME
      # TODO better NSE at some point
      tryCatch({
        rlang::eval_tidy(code)
      }, error = function(e) {
        eval(parse(text=rlang::as_label(code)), envir = this_env)
      })
    }, lazy = TRUE)
  }
  return_func
}

#' The dplyr verbs implemented for disk.frame
#' @description 
#' Please see the dplyr document for their usage. Please note that `group_by`
#' and `arrange` performs the actions within each chunk
#' @export
#' @importFrom dplyr select rename filter mutate transmute arrange do groups group_by group_by glimpse summarise
#' @param ... Same as the dplyr functions
#' @param .data a disk.frame
#' @rdname dplyr_verbs
#' @family dplyr verbs
#' @examples
#' library(dplyr)
#' library(magrittr)
#' cars.df = as.disk.frame(cars)
#' mult = 2
#' 
#' # use all any of the supported dplyr
#' cars2 = cars.df %>% 
#'   select(speed) %>% 
#'   mutate(speed2 = speed * mult) %>% 
#'   filter(speed < 50) %>% 
#'   rename(speed1 = speed) %>% 
#'   collect
#' 
#' # clean up cars.df
#' delete(cars.df)
select.disk.frame <- function(.data, ...) {
  quo_dotdotdot = rlang::enquos(...)
  map(.data, ~{
    code = rlang::quo(dplyr::select(.x, !!!quo_dotdotdot))
    rlang::eval_tidy(code)
  }, lazy = TRUE)
}


#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
rename.disk.frame <- create_dplyr_mapper(dplyr::rename)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
filter.disk.frame <- create_dplyr_mapper(dplyr::filter)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_all
filter_all.disk.frame <- create_dplyr_mapper(dplyr::filter_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_if
filter_if.disk.frame <- create_dplyr_mapper(dplyr::filter_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_at
filter_at.disk.frame <- create_dplyr_mapper(dplyr::filter_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom future getGlobalsAndPackages
#' @importFrom rlang eval_tidy quo enquos
#' @importFrom dplyr mutate
mutate.disk.frame <- create_dplyr_mapper(dplyr::mutate)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr transmute
#' @rdname dplyr_verbs
transmute.disk.frame <- create_dplyr_mapper(dplyr::transmute)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
arrange.disk.frame =create_dplyr_mapper(dplyr::arrange, warning_msg="`arrange.disk.frame` is now deprecated. Please use `chunk_arrange` instead. This is in preparation for a more powerful `arrange` that sorts the whole disk.frame")

#' @family dplyr verbs
#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
chunk_arrange <- create_dplyr_mapper(dplyr::arrange)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr tally
#' @rdname dplyr_verbs
tally.disk.frame <- create_dplyr_mapper(dplyr::tally)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr count
#' @rdname dplyr_verbs
count.disk.frame <- create_dplyr_mapper(dplyr::count)

# TODO alot of these .disk.frame functions are not generic

#' @family dplyr verbs
#' @export
#' @importFrom dplyr add_count
#' @rdname dplyr_verbs
add_count.disk.frame <- create_dplyr_mapper(dplyr::add_count)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr add_tally
#' @rdname dplyr_verbs
add_tally.disk.frame <- create_dplyr_mapper(dplyr::add_tally)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr summarize
#' @rdname dplyr_verbs
chunk_summarize <- create_dplyr_mapper(dplyr::summarize)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr summarise
#' @rdname dplyr_verbs
chunk_summarise <- create_dplyr_mapper(dplyr::summarise)

#' @family dplyr verbs
#' @export
#' @importFrom dplyr summarize
#' @rdname dplyr_verbs
summarize.disk.frame <- create_dplyr_mapper(dplyr::summarize, warning_msg="`summarize.disk.frame` is now deprecated. Please use `chunk_summarize` instead. This is in preparation for a more powerful `group_by` framework")
  #function(...) {
  #stop("`summarize.disk.frame` has been removed. Please use `chunk_summarize` instead. This is in preparation for a more powerful `group_by` framework")
#}

#' @family dplyr verbs
#' @export
#' @importFrom dplyr summarize
#' @rdname dplyr_verbs
summarise.disk.frame <- create_dplyr_mapper(dplyr::summarise, warning_msg="`summarise.disk.frame` is now deprecated. Please use `chunk_summarise` instead. This is in preparation for a more powerful `group_by` framework")
#function(...) {
#   stop("`summarise.disk.frame` has been removed. Please use `chunk_summarise` instead. This is in preparation for a more powerful `group_by` framework")
# }

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr do
do.disk.frame <- create_dplyr_mapper(dplyr::do)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_all
group_by_all.disk.frame <- create_dplyr_mapper(dplyr::group_by_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_at
group_by_at.disk.frame <- create_dplyr_mapper(dplyr::group_by_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_if
group_by_if.disk.frame <- create_dplyr_mapper(dplyr::group_by_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_all
mutate_all.disk.frame <- create_dplyr_mapper(dplyr::mutate_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_at
mutate_at.disk.frame <- create_dplyr_mapper(dplyr::mutate_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_if
mutate_if.disk.frame <- create_dplyr_mapper(dplyr::mutate_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_all
rename_all.disk.frame <- create_dplyr_mapper(dplyr::rename_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_at
rename_at.disk.frame <- create_dplyr_mapper(dplyr::rename_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_if
rename_if.disk.frame <- create_dplyr_mapper(dplyr::rename_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_all
select_all.disk.frame <- create_dplyr_mapper(dplyr::select_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_at
select_at.disk.frame <- create_dplyr_mapper(dplyr::select_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_if
select_if.disk.frame <- create_dplyr_mapper(dplyr::select_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarise_all
chunk_summarise_all <- create_dplyr_mapper(dplyr::summarise_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarise_at
chunk_summarise_at <- create_dplyr_mapper(dplyr::summarise_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize
chunk_summarize <- create_dplyr_mapper(dplyr::summarize)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_all
chunk_summarize_all <- create_dplyr_mapper(dplyr::summarize_all)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_at
chunk_summarize_at <- create_dplyr_mapper(dplyr::summarize_at)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_if
chunk_summarize_if <- create_dplyr_mapper(dplyr::summarize_if)

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr distinct
distinct.disk.frame <- function(...) {
  stop("`distinct.disk.frame` is not available. Please use `chunk_distinct`")
}

#' @family dplyr verbs
#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr distinct
chunk_distinct <- create_dplyr_mapper(dplyr::distinct, warning_msg = "the `distinct` function applies distinct chunk-wise")

#' The shard keys of the disk.frame
#' @return character
#' @export
#' @param x a disk.frame
groups.disk.frame <- function(x){
  shardkey(x)
}

#' Group by within each disk.frame
#' @description
#' The disk.frame group by operation perform group WITHIN each chunk. This is
#' often used for performance reasons. If the user wishes to perform group-by,
#' they may choose to use the `hard_group_by` function which is expensive as it
#' reorganizes the chunks by the shard key.
#' @seealso hard_group_by
#' @param .data a disk.frame
#' @param ... same as the dplyr::group_by
#' @export
#' @family dplyr verbs
#' @rdname group_by
# TODO check shardkey
# group_by.disk.frame <- function(.data, ...) {
#   dplyr_fn = dplyr::group_by
#   
#   quo_dotdotdot = rlang::enquos(...)
#   
#   # this is designed to capture any global stuff
#   vars_and_pkgs = future::getGlobalsAndPackages(quo_dotdotdot)
#   data_for_eval_tidy = force(vars_and_pkgs$globals)
#   
#   res = map(.data, ~{
#     this_env = environment()
#     
#     if(length(data_for_eval_tidy) > 0) {
#       for(i in 1:length(data_for_eval_tidy)) {
#         assign(names(data_for_eval_tidy)[i], data_for_eval_tidy[[i]], pos = this_env)
#       }
#     }
#     
#     lapply(quo_dotdotdot, function(x) {
#       attr(x, ".Environment") = this_env
#     })
#     
#     code = rlang::quo(dplyr_fn(.x, !!!quo_dotdotdot))
#     eval(parse(text=rlang::as_label(code)), envir = this_env)
#   }, lazy = TRUE)
# }
#group_by.disk.frame <- create_dplyr_mapper(dplyr::group_by, warning_msg = "The group_by operation is applied WITHIN each chunk, hence the results may not be as expected. To address this issue, you can rechunk(df, shardby = your_group_keys) which can be computationally expensive. Otherwise, you may use a second stage summary to obtain the desired result.")
group_by.disk.frame <- create_dplyr_mapper(dplyr::group_by, warning_msg="`group_by.disk.frame` is now deprecated. Please use `chunk_group_by` instead. This is in preparation for a more powerful `group_by` framework")
#function(...) {
  #stop("`arrange.disk.frame` has been removed. Please use `chunk_arrange` instead. This is preparation for a more powerful `group_by` framework")
#}

#' @export
#' @family dplyr verbs
#' @rdname group_by
chunk_group_by <- create_dplyr_mapper(dplyr::group_by)

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
play <- function(.data, cmds=NULL) {
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
      .data <- do.call(cmd$func, c(list(.data),cmd$dotdotdot), envir = an_env)
    }
  }
  .data
}
