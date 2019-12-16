#' @export
#' @rdname cmap
map <- function(.x, .f, ...) {
  UseMethod("map")
}

#' @export
#' @rdname cmap
map.disk.frame <- function(...) {
  warning("map(df, ...) where df is a disk.frame has been deprecated. Please use cmap(df,...) instead")
  cmap.disk.frame(...)
}

#' @export
#' @rdname cmap
map.default <- function(.x, .f, ...) {
  purrr::map(.x, .f, ...)
}


#' @export
#' @rdname cmap
imap_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("imap_dfr")
}

#' @export
#' @rdname cmap
imap_dfr.disk.frame <- function(...) {
  warning("imap_dfr(df, ...) where df is disk.frame is deprecated. Please use cimap_dfr(df, ...) instead")
  cimap_dfr.disk.frame(...)
}

#' @export
#' @rdname cmap
imap_dfr.default <- function(.x, .f, ..., .id = NULL) {
  purrr::imap_dfr(.x, .f, ..., .id = .id)
}

#' @export
#' @rdname cmap
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # .x is the chunk and .y is the ID as an integer
#' 
#' # lazy = TRUE support is not available at the moment
#' imap(cars.df, ~.x[, id := .y], lazy = FALSE)
#' 
#' imap_dfr(cars.df, ~.x[, id := .y])
#' 
#' # clean up cars.df
#' delete(cars.df)
imap <- function(.x, .f, ...) {
  UseMethod("imap")
}

imap.disk.frame <- function(...) {
  warning("imap(df,..) where df is disk.frame is deprecated. Use cimap(df, ...) instead")
  cimap.disk.frame(...)
}

#' @export
#' @rdname cmap
imap.default <- function(.x, .f, ...) {
  purrr::imap(.x, .f, ...)
}

#' @rdname cmap
#' @param .id not used
#' @export
map_dfr.disk.frame <- function(...) {
  warning("map_dfr(df, ...) where df is disk.frame is deprecated. Please use cmap_dfr instead")
  cmap_dfr.disk.frame(...)
}

map_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("map_dfr")
}

#' @export
#' @rdname cmap
map_dfr.default <- function(.x, .f, ..., .id = NULL) {
  purrr::map_dfr(.x, .f, ..., .id = .id)
}