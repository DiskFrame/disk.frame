#' Apply the same function to all chunks
#' @param .x a disk.frame
#' @param .f a function to apply to each of the chunks
#' @param outdir the output directory
#' @param keep the columns to keep from the input
#' @param chunks The number of chunks to output
#' @param lazy if TRUE then do this lazily
#' @param compress 0-100 fst compression ratio
#' @param overwrite if TRUE removes any existing chunks in the data
#' @param ... for compatibility with `purrr::map`
#' @param use.names for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param fill for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param idcol for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @import fst
#' @importFrom purrr as_mapper map
#' @importFrom future.apply future_lapply
#' @export
map <- function(.x, .f, ...) {
  UseMethod("map")
}

#' @rdname map
#' @param .id not used
map_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("map_dfr")
}

map_dfr.default <- function(.x, .f, ..., .id = NULL) {
  purrr::map(.x, .f, ..., id)
}


map_dfr.disk.frame <- function(.x, .f, ..., .id = NULL, use.names = fill, fill = FALSE, idcol = NULL) {
  if(!is.null(.id)) {
    warning(".id is not NULL, but the parameter is not used with map_dfr.disk.frame")
  }
  data.table::rbindlist(map.disk.frame(.x, .f, ..., lazy = FALSE), use.names = use.names, fill = fill, idcol = idcol)
}

#' @export
map.default <- function(.x, .f, ...) {
  purrr::map(.x, .f, ...)
}

#' @rdname map
#' @export
map.disk.frame <- function(.x, .f, ..., outdir = NULL, keep = NULL, chunks = nchunks(.x), compress = 50, lazy = T, overwrite = F) {
  #browser()
  .f = purrr::as_mapper(.f)
  if(lazy) {
    attr(.x, "lazyfn") = c(attr(.x, "lazyfn"), .f)
    return(.x)
  }
  
  if(!is.null(outdir)) {
    overwrite_check(outdir, overwrite)
  }
  
  stopifnot(is_ready(.x))
  
  keep1 = attr(.x,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(.x, "path")
  files <- list.files(path, full.names = T)
  files_shortname <- list.files(path)
  
  keep_future = keep
  res = future.apply::future_lapply(1:length(files), function(ii) {
  #res = lapply(1:length(files), function(ii) {
    #browser()
    ds = disk.frame::get_chunk(.x, ii, keep=keep_future)
    res = .f(ds)
    if(!is.null(outdir)) {
      fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  })
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}


imap_dfr <- function(.x, .f, ..., .id = NULL) {
  
}

#' @export
#' 
imap_dfr.disk.frame <- function(.x, .f, ..., .id = NULL) {
  
}

#' imap.disk.frame accepts a two argument function where the first argument is a disk.frame and the 
#' second is the chunk ID
#' @export
#' @rdname map
imap.disk.frame <- function(.x, .f, outdir = NULL, keep = NULL, chunks = nchunks(.x), compress = 50, lazy = T, overwrite = F) {
  ##browser
  .f = purrr::as_mapper(.f)
  
  if(lazy) {
    attr(.x, "lazyfn") = c(attr(.x, "lazyfn"), .f)
    return(.x)
  }
  
  if(!is.null(outdir)) {
    overwrite_check(outdir, overwrite)
  }
  
  stopifnot(is_ready(.x))
  
  keep1 = attr(.x,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(.x, "path")
  files <- list.files(path, full.names = T)
  files_shortname <- list.files(path)
  
  keep_future = keep
  res = future.apply::future_lapply(1:length(files), function(ii) {
    ds = disk.frame::get_chunk(.x, ii, keep=keep_future)
    res = .f(ds, ii)
    if(!is.null(outdir)) {
      fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  })
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}

#' `lazy` is convenience function to apply `.f` to every chunk
#' @export
#' @rdname map
lazy <- function(.x, .f, ...) {
  UseMethod("lazy")
}

#' @rdname map
lazy.disk.frame <- function(.x, .f, ...) {
  map.disk.frame(.x, .f, ..., lazy = T)
}

#' Lazy chunk_lapply wrapper
#' @export
#' @rdname map
delayed <- function(.x, .f, ...) {
  UseMethod("delayed")
}

#' @export
#' @rdname map
delayed.disk.frame <- function(.x, .f, ...) {
  map.disk.frame(.x, .f, ..., lazy = T)
}
  
#' @export
#' @rdname map
chunk_lapply <- function (...) {
  warning("chunk_lapply is deprecated in favour of map.disk.frame")
  map.disk.frame(...)
}