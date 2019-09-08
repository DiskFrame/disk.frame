#' Shard a data.frame/data.table or disk.frame into chunk and saves it into a disk.frame
#' @param df A data.frame/data.table or disk.frame. If disk.frame, then rechunk(df, ...) is run
#' @param shardby The column(s) to shard the data by.
#' @param nchunks The number of chunks
#' @param outdir The output directory of the disk.frame
#' @param overwrite If TRUE then the chunks are overwritten
#' @param ... not used
#' @importFrom data.table setDT
#' @importFrom glue glue
#' @export
#' @examples
#' 
#' # shard the cars data.frame by speed so that rows with the same speed are in the same chunk
#' iris.df = shard(iris, "Species")
#'
#' # clean up cars.df
#' delete(iris.df)
shard <- function(df, shardby, outdir = tempfile(fileext = ".df"), ..., nchunks = recommend_nchunks(df), overwrite = FALSE) {
  force(nchunks)
  overwrite_check(outdir, overwrite)
  
  
  if("data.frame" %in% class(df)) {
    data.table::setDT(df)
    if(length(shardby) == 1) {
      code = glue::glue("df[,.out.disk.frame.id := hashstr2i(as.character({shardby}), nchunks)]")
    } else {
      shardby_list = glue::glue("paste0({paste0(sort(shardby),collapse=',')})")
      code = glue::glue("df[,.out.disk.frame.id := hashstr2i({shardby_list}, nchunks)]")
    }
    
    tryCatch(
      eval(parse(text=code)),
      error = function(e) {
        message("error occurred in shard")
      }
    )
    
    stopifnot(".out.disk.frame.id" %in% names(df))
    
    res = write_disk.frame(df, outdir = outdir, nchunks = nchunks, overwrite = TRUE, shardby = shardby, shardchunks = nchunks)  
    return(res)
  } else if ("disk.frame" %in% class(df)){
    nchunks_rechunk = nchunks
    return(rechunk(df, shardby = shardby, nchunks = nchunks_rechunk, outdir = outdir, overwrite = TRUE))
  }
}

#' `distribute` is an alias for `shard`
#' @export
#' @rdname shard
distribute <- function(...) {
  UseMethod("shard")
}
