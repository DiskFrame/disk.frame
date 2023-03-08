#' The dplyr verbs implemented for disk.frame
#' @description Please see the dplyr document for their usage. Please note
#' `chunk_arrange` performs the actions within each chunk
#' @export
#' @importFrom dplyr select rename filter mutate transmute arrange do groups
#'   group_by group_by glimpse summarise
#' @param ... Same as the dplyr functions
#' @param .data a disk.frame
#' @param x `dplyr::glimpse` parameter
#' @param width `dplyr::glimpse` parameter
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
select.disk.frame <- create_chunk_mapper(dplyr::select)


#' @export
#' @rdname dplyr_verbs
rename.disk.frame <- create_chunk_mapper(dplyr::rename)


#' @export
#' @rdname dplyr_verbs
filter.disk.frame <- create_chunk_mapper(dplyr::filter)

#' @export
#' @rdname dplyr_verbs
#' @importFrom future getGlobalsAndPackages
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

# TODO family is not required is group-by
# TODO alot of these .disk.frame functions are not generic


# TODO make this work like in dplyr
#' #' @export
#' #' @importFrom dplyr add_count
#' #' @rdname dplyr_verbs
#' add_count.disk.frame <- create_chunk_mapper(dplyr::add_count)


#' #' @export
#' #' @importFrom dplyr add_tally
#' #' @rdname dplyr_verbs
#' add_tally.disk.frame <- create_chunk_mapper(dplyr::add_tally)


#' @export
#' @importFrom dplyr summarize
#' @rdname chunk_group_by
chunk_summarize <- create_chunk_mapper(dplyr::summarize)


#' @export
#' @importFrom dplyr summarise
#' @rdname chunk_group_by
chunk_summarise <- create_chunk_mapper(dplyr::summarise)


#' #' @export
#' #' @rdname dplyr_verbs
#' #' @importFrom dplyr do
#' do.disk.frame <- create_chunk_mapper(dplyr::do)


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
#' @seealso hard_group_by group_by
#' @param .data a disk.frame
#' @param ... passed to dplyr::group_by
#' @export
#' @rdname chunk_group_by
#' @export
chunk_group_by <- create_chunk_mapper(dplyr::group_by)

#' @rdname chunk_group_by
#' @export
chunk_ungroup = create_chunk_mapper(dplyr::ungroup)

# do not introduce it as it was never introduced
#ungroup.disk.frame( < - create_dplyr_mapper(dplyr::ungroup, , warning_msg="`ungroup.disk.frame` is now deprecated. Please use `chunk_ungroup` instead. This is in preparation for a more powerful `group_by` framework")



#' @export
#' @rdname dplyr_verbs
glimpse.disk.frame <- function(x, width = NULL, ...) {
  glimpse(head(x, ...), width = width, ...)
}
