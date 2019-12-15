#' Verbs from maditr
#' @rdname maditr_verbs
#' @importFrom maditr let
#' @export
let.disk.frame <- create_chunk_mapper(maditr::let, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_mutate
dt_mutate.disk.frame <- create_chunk_mapper(maditr::mutate, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_summarize
chunk_dt_summarize.disk.frame <- create_chunk_mapper(maditr::dt_summarize, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_filter
dt_filter.disk.frame <- create_chunk_mapper(maditr::filter, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_select
dt_select.disk.frame <- create_chunk_mapper(maditr::select, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_arrange
chunk_dt_arrange.disk.frame <- create_chunk_mapper(maditr::dt_arrange, as.data.frame = FALSE)

#' @export
#' @importFrom maditr take
take.disk.frame <- create_chunk_mapper(maditr::take, as.data.frame = FALSE)

#' @export
#' @importFrom maditr take_if
take_if.disk.frame <- create_chunk_mapper(maditr::take_if, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_inner_join
dt_inner_join.disk.frame <- create_chunk_mapper(maditr::dt_inner_join.disk.frame, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_left_join
dt_left_join.disk.frame <- create_chunk_mapper(maditr::dt_left_join.disk.frame, as.data.frame = FALSE)

#' @export
#' @importFrom maditr dt_full_join
dt_full_join.disk.frame <- create_chunk_mapper(maditr::dt_full_join.disk.frame, as.data.frame = FALSE)
