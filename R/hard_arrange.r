#' #' Perform a hard arrange
#' #' @description
#' #' A hard_arrange is a sort by that also reorganizes the chunks to ensure that
#' #' every unique grouping of `by`` is in the same chunk. Or in other words, every
#' #' row that share the same `by` value will end up in the same chunk.
#' #' @param df a disk.frame
#' #' @param ... grouping variables
#' #' @param outdir the output directory
#' #' @param nchunks The number of chunks in the output. Defaults = nchunks.disk.frame(df)
#' #' @param overwrite overwrite the out put directory
#' #' @param add same as dplyr::arrange
#' #' @param .drop same as dplyr::arrange
#' #' @export
#' #' @examples
#' #' iris.df = as.disk.frame(iris, nchunks = 2)
#' #' 
#' #' # arrange iris.df by specifies and ensure rows with the same specifies are in the same chunk
#' #' iris_hard.df = hard_arrange(iris.df, Species)
#' #' 
#' #' get_chunk(iris_hard.df, 1)
#' #' get_chunk(iris_hard.df, 2)
#' #' 
#' #' # clean up cars.df
#' #' delete(iris.df)
#' #' delete(iris_hard.df)
#' hard_arrange <- function(df, ..., add = FALSE, .drop = FALSE) {
#'   UseMethod("hard_arrange")
#' }
#' 
#' #' @rdname hard_arrange
#' #' @export
#' #' @importFrom dplyr arrange
#' hard_arrange.data.frame <- function(df, ...) {
#'   dplyr::arrange(df, ...)
#' }
#' 
#' #' @rdname hard_arrange
#' #' @importFrom purrr map
#' #' @export
#' hard_arrange.disk.frame <- function(df, ..., outdir=tempfile("tmp_disk_frame_hard_arrange"), nchunks = disk.frame::nchunks(df), overwrite = TRUE) {
#'   overwrite_check(outdir, overwrite)
#'   
#'   # Refer also to Dplyr arrange: https://github.com/tidyverse/dplyr/blob/master/src/arrange.cpp
#'   q <- enquos(...)
#'   is_sym <- sapply(q, rlang::quo_is_symbol)
#'   arrange_codes <- sapply(q, rlang::as_label)
#'   
#'   # Check if desc...
#'   is_desc <- substr(arrange_codes, 1, 5) == "desc("
#'   
#'   # If expr is a symbol from the data, just use it.
#'   # Otherwise need to evaluate ... 
#'   # (TODO - currently only support variables and desc in the data)
#'   # Peels off "desc" from the original
#'   vars <- sub(")", "", sub("desc(", "", arrange_codes, fixed=TRUE), fixed=TRUE)
#' 
#'   desc_vars <- vars[is_desc]
#'   
#'   if(!all(vars %in% colnames(df))){
#'     stop(paste0("Expressions currently not supported. Columns not found in colnames:", vars[!vars %in% colnames(df)]))
#'   }
#'   
#'   # Hard group by in a partially sorted way at the chunk level and then arrange within chunks
#'   df %>% 
#'     disk.frame::hard_group_by(vars, outdir=outdir, nchunks=nchunks, overwrite=overwrite, shardby_function="sort", desc_vars=desc_vars) %>%
#'     chunk_arrange(...)
#' }