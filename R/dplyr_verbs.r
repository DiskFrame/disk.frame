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

#' Kept for backwards-compatibility to be removed in 0.3
#' @export
create_dplyr_mapper = create_chunk_mapper

#' @export
#' @rdname dplyr_verbs
rename.disk.frame <- create_chunk_mapper(dplyr::rename)


#' @export
#' @rdname dplyr_verbs
filter.disk.frame <- create_chunk_mapper(dplyr::filter)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_all
filter_all.disk.frame <- create_chunk_mapper(dplyr::filter_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_if
filter_if.disk.frame <- create_chunk_mapper(dplyr::filter_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr filter_at
filter_at.disk.frame <- create_chunk_mapper(dplyr::filter_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom future getGlobalsAndPackages
#' @importFrom rlang eval_tidy quo enquos
#' @importFrom dplyr mutate
mutate.disk.frame <- create_chunk_mapper(dplyr::mutate)


#' @export
#' @importFrom dplyr transmute
#' @rdname dplyr_verbs
transmute.disk.frame <- create_chunk_mapper(dplyr::transmute)


#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
arrange.disk.frame =create_chunk_mapper(dplyr::arrange, warning_msg="`arrange.disk.frame` is now deprecated. Please use `chunk_arrange` instead. This is in preparation for a more powerful `arrange` that sorts the whole disk.frame")


#' @export
#' @importFrom dplyr arrange
#' @rdname dplyr_verbs
chunk_arrange <- create_chunk_mapper(dplyr::arrange)


#' @export
#' @importFrom dplyr tally
#' @rdname dplyr_verbs
tally.disk.frame <- create_chunk_mapper(dplyr::tally)


#' @export
#' @importFrom dplyr count
#' @rdname dplyr_verbs
count.disk.frame <- create_chunk_mapper(dplyr::count)

# TODO family is not required is group-by
# TODO alot of these .disk.frame functions are not generic


#' @export
#' @importFrom dplyr add_count
#' @rdname dplyr_verbs
add_count.disk.frame <- create_chunk_mapper(dplyr::add_count)


#' @export
#' @importFrom dplyr add_tally
#' @rdname dplyr_verbs
add_tally.disk.frame <- create_chunk_mapper(dplyr::add_tally)


#' @export
#' @importFrom dplyr summarize
#' @rdname dplyr_verbs
chunk_summarize <- create_chunk_mapper(dplyr::summarize)


#' @export
#' @importFrom dplyr summarise
#' @rdname dplyr_verbs
chunk_summarise <- create_chunk_mapper(dplyr::summarise)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr do
do.disk.frame <- create_chunk_mapper(dplyr::do)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_all
group_by_all.disk.frame <- create_chunk_mapper(dplyr::group_by_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_at
group_by_at.disk.frame <- create_chunk_mapper(dplyr::group_by_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr group_by_if
group_by_if.disk.frame <- create_chunk_mapper(dplyr::group_by_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_all
mutate_all.disk.frame <- create_chunk_mapper(dplyr::mutate_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_at
mutate_at.disk.frame <- create_chunk_mapper(dplyr::mutate_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr mutate_if
mutate_if.disk.frame <- create_chunk_mapper(dplyr::mutate_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_all
rename_all.disk.frame <- create_chunk_mapper(dplyr::rename_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_at
rename_at.disk.frame <- create_chunk_mapper(dplyr::rename_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr rename_if
rename_if.disk.frame <- create_chunk_mapper(dplyr::rename_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_all
select_all.disk.frame <- create_chunk_mapper(dplyr::select_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_at
select_at.disk.frame <- create_chunk_mapper(dplyr::select_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr select_if
select_if.disk.frame <- create_chunk_mapper(dplyr::select_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarise_all
chunk_summarise_all <- create_chunk_mapper(dplyr::summarise_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarise_at
chunk_summarise_at <- create_chunk_mapper(dplyr::summarise_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize
chunk_summarize <- create_chunk_mapper(dplyr::summarize)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_all
chunk_summarize_all <- create_chunk_mapper(dplyr::summarize_all)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_at
chunk_summarize_at <- create_chunk_mapper(dplyr::summarize_at)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr summarize_if
chunk_summarize_if <- create_chunk_mapper(dplyr::summarize_if)


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr distinct
distinct.disk.frame <- function(...) {
  stop("`distinct.disk.frame` is not available. Please use `chunk_distinct`")
}


#' @export
#' @rdname dplyr_verbs
#' @importFrom dplyr distinct
chunk_distinct <- create_chunk_mapper(dplyr::distinct, warning_msg = "the `distinct` function applies distinct chunk-wise")

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
#' @rdname group_by
#' @export
chunk_group_by <- create_chunk_mapper(dplyr::group_by)

#' @export
chunk_ungroup = create_chunk_mapper(dplyr::ungroup)

# do not introduce it as it was never introduced
#ungroup.disk.frame( < - create_dplyr_mapper(dplyr::ungroup, , warning_msg="`ungroup.disk.frame` is now deprecated. Please use `chunk_ungroup` instead. This is in preparation for a more powerful `group_by` framework")


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
