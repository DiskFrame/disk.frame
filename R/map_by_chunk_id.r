#' Get the chunk file names
#' @param df a disk.frame
#' @param full.names If TRUE returns the full path to the file, Defaults to F.
#' @param strip_extension If TRUE then the file extentsion in the chunk_id is removed. Defaults to TRUE
#' @import stringr
get_chunk_ids <- function(df, full.names = F, strip_extension = T) {
  lf = list.files(attr(df,"path"), full.names = full.names, ...)
  if(full.names) {
    return(lf)
  }
  purrr::map_chr(lf, ~{
    tmp = stringr::str_split(.x,stringr::fixed("."), simplify = T)
    l = length(tmp)
    if(l == 1) {
      return(tmp)
    } else if(strip_extension) {
      paste0(tmp[-l], collapse="")
    } else if (l==1) {
      paste0(tmp[-l], collapse="")
    }
  })
}

#' Perform a function on both disk.frames x and y, each chunk of x and y gets run by fn(x.chunk, y.chunk)
#' @param x a disk.frame
#' @param y a disk.frame
#' @param fn a function to be called on each chunk of x and y matched by chunk_id
#' @param outdir output directory
#' @import stringr purrr fst
#' @importFrom data.table data.table
#' @export
map_by_chunk_id <- function(x, y, fn, outdir) {
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

