#' Shard a data.frame/data.table or disk.frame into chunk and saves it into a disk.frame
#' @param df A data.frame/data.table or disk.frame. If disk.frame, then rechunk(df, ...) is run
#' @param shardby The column(s) to shard the data by.
#' @param nchunks The number of chunks
#' @param outdir The output directory of the disk.frame
#' @param overwrite If TRUE then the chunks are overwritten
#' @param shardby_function splitting of chunks: "hash" for hash function or "sort" for semi-sorted chunks
#' @param sort_splits If shardby_function is "sort", the split values for sharding
#' @param desc_vars for the "sort" shardby function, the variables to sort descending.
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
shard <- function(df, shardby, outdir = tempfile(fileext = ".df"), ..., nchunks = recommend_nchunks(df), overwrite = FALSE, shardby_function="hash", sort_splits=NULL, desc_vars=NULL) {
  force(nchunks)
  overwrite_check(outdir, overwrite)
  stopifnot(shardby_function %in% c("hash", "sort"))
  
  if("data.frame" %in% class(df)) {
    data.table::setDT(df)
    if(shardby_function == "hash"){
      message("Hashing...")
      if(length(shardby) == 1) {
        code = glue::glue("df[,.out.disk.frame.id := hashstr2i(as.character({shardby}), nchunks)]")
      } else {
        shardby_list = glue::glue("paste0({paste0(sort(shardby),collapse=',')})")
        code = glue::glue("df[,.out.disk.frame.id := hashstr2i({shardby_list}, nchunks)]")
      }
    } else if(shardby_function == "sort"){
      if(nchunks == 1){
        message("Only one chunk: set .out.disk.frame.id = 0")
        code = glue::glue("df[,.out.disk.frame.id := 0]")
      } else {
        shard_by_rule <- sortablestr2i(sort_splits, desc_vars)
        message(shard_by_rule)
        code = glue::glue("df[,.out.disk.frame.id := {shard_by_rule}]")
      }
    }

    tryCatch(
      eval(parse(text=code)),
      error = function(e) {
        message("error occurred in shard")
      }
    )
    
    stopifnot(".out.disk.frame.id" %in% names(df))
    
    res = write_disk.frame(df, outdir = outdir, nchunks = nchunks, overwrite = TRUE, shardby = shardby, shardchunks = nchunks, shardby_function=shardby_function, sort_splits=sort_splits, desc_vars=desc_vars)  
    return(res)
  } else if ("disk.frame" %in% class(df)){
    nchunks_rechunk = nchunks
    return(rechunk(df, shardby = shardby, nchunks = nchunks_rechunk, outdir = outdir, overwrite = TRUE, shardby_function=shardby_function, sort_splits=sort_splits, desc_vars=desc_vars))
  }
}

#' `distribute` is an alias for `shard`
#' @export
#' @rdname shard
distribute <- function(...) {
  UseMethod("shard")
}
