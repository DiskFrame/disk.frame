#' @importFrom tidytable dt_filter
#' @export
dt_filter.disk.frame <- create_chunk_mapper(tidytable::dt_filter, as.data.frame=FALSE)

#' @importFrom tidytable dt_mutate
#' @export
dt_mutate.disk.frame <- create_chunk_mapper(tidytable::dt_mutate, as.data.frame=FALSE)

#' @importFrom tidytable dt_select
#' @export
dt_select.disk.frame <- create_chunk_mapper(tidytable::dt_select, as.data.frame=FALSE)
