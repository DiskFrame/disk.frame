#' Perform a function on both disk.frames x and y, each chunk of x and y gets run by fn(x.chunk, y.chunk)
#' @param .x a disk.frame
#' @param .y a disk.frame
#' @param .f a function to be called on each chunk of x and y matched by chunk_id
#' @param ... not used
#' @param outdir output directory
#' @import stringr fst
#' @importFrom purrr as_mapper map2
#' @importFrom data.table data.table
#' @export
map2 <- function(.x, .y, .f, ...){
  UseMethod("map2")
}

#' @export
map2.default <- function(.x, .y, .f, ...) {
  purrr::map2(.x,.y,.f,...)
}

#' @export
map2.disk.frame <- function(.x,.y,.f, ..., outdir) {
  fn = purrr::as_mapper(fn)
  fs::dir_create(outdir)
  
  # get all the chunk ids
  xc = data.table(cid = get_chunk_ids(x))
  xc[,xid:=get_chunk_ids(x, full.names = T)]
  yc = data.table(cid = get_chunk_ids(y))
  yc[,yid:=get_chunk_ids(y, full.names = T)]
  
  xyc = merge(xc, yc, by="cid", all = T, allow.cartesian = T)
  
  # apply the functions
  future.apply::future_mapply(function(xid, yid, outid) {
    xch = disk.frame::get_chunk(x, xid, full.names = T)
    ych = disk.frame::get_chunk(y, yid, full.names = T)
    xych = fn(xch, ych)
    if(base::nrow(xych) > 0) {
      fst::write_fst(xych, file.path(outdir, paste0(outid,".fst")))
    } else {
      warning(glue::glue("one of the chunks, {xid}, is empty"))
    }
    NULL
  }, xyc$xid, xyc$yid, xyc$cid)
  
  disk.frame(outdir)
}

#' @rdname map2
map_by_chunk_id <- function(.x, .y, .f, ..., outdir) {
  warning("map_by_chunk_id is deprecated. Use map2 instead")
  map2.disk.frame(.x, .y, .f, ..., outdir)
}

