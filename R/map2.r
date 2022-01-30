#' `cmap2` a function to two disk.frames
#' @description
#' Perform a function on both disk.frames .x and .y, each chunk of .x and .y
#' gets run by .f(x.chunk, y.chunk)
#' @param .x a disk.frame
#' @param .y a disk.frame
#' @param .f a function to be called on each chunk of x and y matched by
#'   chunk_id
#' @param ... not used
#' @param outdir output directory
#' @import stringr fst
#' @importFrom purrr as_mapper map2
#' @importFrom data.table data.table
#' @export
#' @rdname cmap2
#' @examples
#' cars.df = as.disk.frame(cars)
#'
#' cars2.df = cmap2(cars.df, cars.df, ~data.table::rbindlist(list(.x, .y)))
#' collect(cars2.df)
#'
#' # clean up cars.df
#' delete(cars.df)
#' delete(cars2.df)
cmap2 <- function(.x, .y, .f, ...){
  UseMethod("cmap2")
}


#' @export
#' @importFrom pryr do_call
cmap2.disk.frame <- function(.x, .y, .f, ..., outdir = tempfile(fileext = ".df"), .progress = TRUE) {
  if(!"disk.frame" %in% class(.x)) {
    code = deparse(substitute(cmap2.disk.frame(.x,.y, ...))) %>% paste(collapse = "\n")
    stop(sprintf("running %s : the .x argument must be a disk.frame", code))
  } 
  
  .f = purrr::as_mapper(.f)
  
  if("disk.frame" %in% class(.y)) {
    fs::dir_create(outdir)
    
    # get all the chunk ids
    xc = data.table(cid = get_chunk_ids(.x))
    xc[,xid:=get_chunk_ids(.x, full.names = TRUE)]
    yc = data.table(cid = get_chunk_ids(.y))
    yc[,yid:=get_chunk_ids(.y, full.names = TRUE)]
    
    xyc = merge(xc, yc, by="cid", all = TRUE, allow.cartesian = TRUE)
    # apply the functions
    
    future.apply::future_mapply(function(xid, yid, outid) {
    #mapply(function(xid, yid, outid) {
      xch = disk.frame::get_chunk(.x, xid, full.names = TRUE)
      ych = disk.frame::get_chunk(.y, yid, full.names = TRUE)
      xych = .f(xch, ych)
      if(base::nrow(xych) > 0) {
        fst::write_fst(xych, file.path(outdir, paste0(outid,".fst")))
      } else {
        warning(sprintf("one of the chunks, %s, is empty", xid))
      }
      NULL
    }, xyc$xid, xyc$yid, xyc$cid # together with mapply
    , future.seed=NULL
    )
    
    return(disk.frame(outdir))
  } else {
    # if .y is not a disk.frame
    warning("in cmap2(.x,.y,...) the .y is not a disk.frame, so returning a list instead of a disk.frame")
    
    f_for_passing = force(.f)
    tmp_disk.frame = force(.x)
    res = future.apply::future_mapply(function(xs, ys, ...) {
      ddd = c(list(get_chunk(tmp_disk.frame, xs, full.names = TRUE), ys), ...)
      do.call(f_for_passing, ddd)
    }, get_chunk_ids(tmp_disk.frame, full.names = TRUE), .y, ..., SIMPLIFY=FALSE, future.seed = TRUE)
    
    
    return(res)
  }
}

