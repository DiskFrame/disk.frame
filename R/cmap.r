#' Apply the same function to all chunks
#' @param .x a disk.frame
#' @param .f a function to apply to each of the chunks
#' @param outdir the output directory
#' @param lazy if TRUE then do this lazily
#' @param use.names for cmap_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param fill for cmap_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param idcol for cmap_dfr's call to data.table::rbindlist. See data.table::rbindlist
#' @param ... Passed to `collect` and `write_disk.frame`
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # return the first row of each chunk lazily
#' # 
#' cars2 = cmap(cars.df, function(chunk) {
#'  chunk[,1]
#' })
#' 
#' collect(cars2)
#'
#' # same as above but using purrr 
#' cars2 = cmap(cars.df, ~.x[1,])
#'
#' collect(cars2)
#' 
#' # return the first row of each chunk eagerly as list
#' cmap(cars.df, ~.x[1,], lazy = FALSE)
#'
#' # return the first row of each chunk eagerly as data.table/data.frame by row-binding
#' cmap_dfr(cars.df, ~.x[1,])
#'
#' # lazy and delayed are just an aliases for cmap(..., lazy = TRUE)
#' collect(lazy(cars.df, ~.x[1,]))
#' collect(delayed(cars.df, ~.x[1,]))
#' 
#' # clean up cars.df
#' delete(cars.df)
cmap <- function(.x, .f, ...) {
  UseMethod("cmap")
}


#' @rdname cmap
#' @importFrom future getGlobalsAndPackages
#' @export
cmap.disk.frame <- function(
                    .x, 
                    .f, 
                    ...) {
  .f = purrr::as_mapper(.f)
  
  result = create_chunk_mapper(.f)(.x, ...)
  return(result)
}

#' @export
#' @rdname cmap
cmap_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("cmap_dfr")
}

#' @export
#' @rdname cmap
cmap_dfr.disk.frame <- function(.x, .f, ..., .id = NULL, use.names = fill, fill = FALSE, idcol = NULL) {
  if(!is.null(.id)) {
    warning(".id is not NULL, but the parameter is not used with cmap_dfr.disk.frame")
  }
  
  list_df = collect_list(cmap.disk.frame(.x, .f, ...))
  data.table::rbindlist(list_df, use.names = use.names, fill = fill, idcol = idcol)
}


#' @export
#' @rdname cmap
cimap <- function(.x, .f, ...) {
  UseMethod("cimap")
}

#' `cimap.disk.frame` accepts a two argument function where the first argument is a data.frame and the 
#' second is the chunk ID
#' @export
#' @rdname cmap
cimap.disk.frame <- function(.x, .f, outdir = NULL, keep = NULL, chunks = nchunks(.x), compress = 50, lazy = TRUE, overwrite = FALSE, ...) {
  .f = purrr_as_mapper(.f)
  
  # TODO support lazy for cimap
  if(lazy) {
    stop("cimap.disk.frame: lazy = TRUE is not supported at this stage")
    attr(.x, "recordings") = c(attr(.x, "recordings"), .f)
    return(.x)
  }
  
  if(!is.null(outdir)) {
    overwrite_check(outdir, overwrite)
  }
  
  stopifnot(is_ready(.x))
  
  keep1 = attr(.x,"keep", exact=TRUE)
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(.x, "path")
  files <- get_chunk_ids(.x)
  files_shortname <- list.files(path)
  
  keep_future = keep
  res = future.apply::future_lapply(1:length(files), function(ii) {
    ds = disk.frame::get_chunk(.x, files_shortname[ii], keep=keep_future)
    res = .f(ds, ii)
    if(!is.null(outdir)) {
      fst::write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  }, future.seed = TRUE)
  
  if(!is.null(outdir)) {
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}

#' @export
#' @rdname cmap
cimap_dfr <- function(.x, .f, ..., .id = NULL) {
  UseMethod("cimap_dfr")
}

#' @export
#' @rdname cmap
cimap_dfr.disk.frame <- function(.x, .f, ..., .id = NULL, use.names = fill, fill = FALSE, idcol = NULL) {
  if(!is.null(.id)) {
    warning(".id is not NULL, but the parameter is not used with cmap_dfr.disk.frame")
  }
  data.table::rbindlist(cimap.disk.frame(.x, .f, ..., lazy = FALSE), use.names = use.names, fill = fill, idcol = idcol)
}


#' `lazy` is convenience function to apply `.f` to every chunk
#' @export
#' @rdname cmap
lazy <- function(.x, .f, ...) {
  UseMethod("lazy")
}

#' @rdname cmap
#' @export
lazy.disk.frame <- function(.x, .f, ...) {
  cmap.disk.frame(.x, .f, ..., lazy = TRUE)
}

#' `delayed` is an alias for lazy and is consistent with the naming in Dask and Dagger.jl
#' @export
#' @rdname cmap
delayed <- function(.x, .f, ...) {
  UseMethod("delayed")
}

#' @export
delayed.disk.frame <- function(.x, .f, ...) {
  cmap.disk.frame(.x, .f, ..., lazy = TRUE)
}
  
#' @export
#' @rdname cmap
clapply <- function (...) {
  cmap.disk.frame(...)
}
