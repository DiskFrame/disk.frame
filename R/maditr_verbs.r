#' Verbs from maditr
#' @rdname maditr_verbs
#' @importFrom maditr let
#' @export
let.disk.frame <- create_chunk_mapper(maditr::let, as.data.frame = FALSE)

take.disk.frame <- create_chunk_mapper(maditr::take, as.data.frame = FALSE)

take_if.disk.frame <- create_chunk_mapper(maditr::take_if, as.data.frame = FALSE)