#' Apply the same function to all chunks
#' @param .x a disk.frame
#' @param .f a function to apply to each of the chunks
#' @param outdir the output directory
#' @param keep the columns to keep from the input
#' @param chunks The number of chunks to output
#' @param lazy if TRUE then do this lazily
#' @param compress 0-100 fst compression ratio
#' @param overwrite if TRUE removes any existing chunks in the data
#' @param use.names for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param fill for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param idcol for map_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param vars_and_pkgs variables and packages to send to a background session. This is typically automatically detected
#' @param .progress A logical, for whether or not to print a progress bar for multiprocess, multisession, and multicore plans. From {furrr}
#' @param ... for compatibility with `purrr::map`
#' @import fst
#' @importFrom purrr as_mapper map
#' @importFrom future.apply future_lapply
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # return the first row of each chunk lazily
#' # 
#' cars2 = map(cars.df, function(chunk) {
#'  chunk[,1]
#' })
#' 
#' collect(cars2)
#'
#' # same as above but using purrr 
#' cars2 = map(cars.df, ~.x[1,])
#'
#' collect(cars2)
#' 
#' # return the first row of each chunk eagerly as list
#' map(cars.df, ~.x[1,], lazy = FALSE)
#'
#' # return the first row of each chunk eagerly as data.table/data.frame by row-binding
#' map_dfr(cars.df, ~.x[1,])
#'
#' # lazy and delayed are just an aliases for map(..., lazy = TRUE)
#' collect(lazy(cars.df, ~.x[1,]))
#' collect(delayed(cars.df, ~.x[1,]))
#' 
#' # clean up cars.df
#' delete(cars.df)
map <- function(.x, .f, ...) {
  UseMethod("map")
}

#' @export
map.default <- function(.x, .f, ...) {
  purrr::map(.x, .f, ...)
}

#' @rdname map
#' @importFrom future getGlobalsAndPackages
#' @export
map.disk.frame <- function(.x, .f, ..., outdir = NULL, keep = NULL, chunks = nchunks(.x), compress = 50, lazy = TRUE, overwrite = FALSE, vars_and_pkgs = future::getGlobalsAndPackages(.f, envir = parent.frame()), .progress = TRUE) {
  .f = purrr::as_mapper(.f)
  if(lazy) {
    attr(.x, "lazyfn") = 
      c(
        attr(.x, "lazyfn"), 
        list(
          list(
            func = .f, 
            vars_and_pkgs = vars_and_pkgs, 
            dotdotdot = list(...)
          )
        )
      )
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
  files <- list.files(path, full.names = TRUE)
  files_shortname <- list.files(path)
  
  keep_future = keep
  
  cid = get_chunk_ids(.x, full.names = TRUE)
  
  dotdotdot = list(...)
  
  res = future.apply::future_lapply(1:length(files), function(ii, ...) {
    #res = lapply(1:length(files), function(ii) {
    ds = disk.frame::get_chunk(.x, cid[ii], keep=keep_future, full.names = TRUE)
    
    res = .f(ds, ...)
    
    #res = do.call(.f, c(ds, dotdotdot))
    
    if(!is.null(outdir)) {
      if(nrow(res) == 0) {
        warning(glue::glue("The output chunk has 0 row, therefore chunk {ii} NOT written"))
      } else {
        fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      }
      return(ii)
    } else {
      return(res)
    }
  }, ...)
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}


#' @rdname map
#' @param .id not used
#' @export
map_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("map_dfr")
}

#' @export
#' @rdname map
map_dfr.default <- function(.x, .f, ..., .id = NULL) {
  purrr::map_dfr(.x, .f, ..., .id = .id)
}

#' @export
#' @rdname map
map_dfr.disk.frame <- function(.x, .f, ..., .id = NULL, use.names = fill, fill = FALSE, idcol = NULL) {
  if(!is.null(.id)) {
    warning(".id is not NULL, but the parameter is not used with map_dfr.disk.frame")
  }
  
  # TODO warn the user if outdir is map_dfr
  
  data.table::rbindlist(map.disk.frame(.x, .f, ..., outdir = NULL, lazy = FALSE), use.names = use.names, fill = fill, idcol = idcol)
}


#' @export
#' @rdname map
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
#' @rdname map
imap <- function(.x, .f, ...) {
  UseMethod("imap")
}

#' @export
#' @rdname map
imap.default <- function(.x, .f, ...) {
  purrr::imap(.x, .f, ...)
}

#' `imap.disk.frame` accepts a two argument function where the first argument is a data.frame and the 
#' second is the chunk ID
#' @export
#' @rdname map
imap.disk.frame <- function(.x, .f, outdir = NULL, keep = NULL, chunks = nchunks(.x), compress = 50, lazy = TRUE, overwrite = FALSE, ...) {
  .f = purrr::as_mapper(.f)
  
  # TODO support lazy for imap
  if(lazy) {
    stop("imap.disk.frame: lazy = TRUE is not supported at this stage")
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
  files <- get_chunk_ids(.x)
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

#' @export
#' @rdname map
imap_dfr.disk.frame <- function(.x, .f, ..., .id = NULL, use.names = fill, fill = FALSE, idcol = NULL) {
  if(!is.null(.id)) {
    warning(".id is not NULL, but the parameter is not used with map_dfr.disk.frame")
  }
  data.table::rbindlist(imap.disk.frame(.x, .f, ..., lazy = FALSE), use.names = use.names, fill = fill, idcol = idcol)
}


#' @export
#' @rdname map
imap_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("imap_dfr")
}


#' @export
#' @rdname map
imap_dfr.default <- function(.x, .f, ..., .id = NULL) {
  purrr::imap_dfr(.x, .f, ..., .id = .id)
}


#' `lazy` is convenience function to apply `.f` to every chunk
#' @export
#' @rdname map
lazy <- function(.x, .f, ...) {
  UseMethod("lazy")
}

#' @rdname map
#' @export
lazy.disk.frame <- function(.x, .f, ...) {
  map.disk.frame(.x, .f, ..., lazy = TRUE)
}

#' `delayed` is an alias for lazy and is consistent with the naming in Dask and Dagger.jl
#' @export
#' @rdname map
delayed <- function(.x, .f, ...) {
  UseMethod("delayed")
}

#' @export
#' @rdname map
delayed.disk.frame <- function(.x, .f, ...) {
  map.disk.frame(.x, .f, ..., lazy = TRUE)
}
  
#' @export
#' @rdname map
chunk_lapply <- function (...) {
  warning("chunk_lapply is deprecated in favour of map.disk.frame")
  map.disk.frame(...)
}