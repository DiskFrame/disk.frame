#' Write disk.frame to disk
#' @description
#' Write a data.frame/disk.frame to a disk.frame location. If df is a data.frame
#' then using the as.disk.frame function is recommended for most cases
#' @param diskf a disk.frame
#' @param outdir output directory for the disk.frame
#' @param nchunks number of chunks
#' @param overwrite overwrite output directory
#' @param shardby the columns to shard by
#' @param compress compression ratio for fst files
#' @param shardby_function splitting of chunks: "hash" for hash function or "sort" for semi-sorted chunks
#' @param sort_splits for the "sort" shardby function, a dataframe with the split values.
#' @param desc_vars for the "sort" shardby function, the variables to sort descending. 
#' @param ... passed to cmap.disk.frame
#' @export
#' @import fst fs
#' @importFrom glue glue
#' @examples
#' cars.df = as.disk.frame(cars)
#'
#' # write out a lazy disk.frame to disk
#' cars2.df = write_disk.frame(cmap(cars.df, ~.x[1,]), overwrite = TRUE)
#' collect(cars2.df)
#'
#' # clean up cars.df
#' delete(cars.df)
#' delete(cars2.df)
write_disk.frame <- function(
  diskf,
  outdir = tempfile(fileext = ".df"),
  nchunks = ifelse(
    "disk.frame"%in% class(diskf),
    nchunks.disk.frame(diskf),
    recommend_nchunks(diskf)),
  overwrite = FALSE,
  shardby=NULL, compress = 50, shardby_function="hash", sort_splits=NULL, desc_vars=NULL, ...) {

  force(nchunks)
  overwrite_check(outdir, overwrite)


  if(is.null(outdir)) {
    stop("write_disk.frame error: outdir must not be NULL")
  }

  if(is_disk.frame(diskf)) {
    if(is.null(shardby)) {
      path = attr(diskf, "path")
      files_shortname <- list.files(path)
      cids = get_chunk_ids(diskf, full.names = T, strip_extension = F)
      
      future.apply::future_lapply(1:length(cids), function(ii, ...) {
        chunk = get_chunk(diskf, cids[ii], full.names = TRUE)
        if(nrow(chunk) == 0) {
          warning(sprintf("The output chunk has 0 row, therefore chunk %d NOT written", ii))
        } else {
          out_chunk_name = file.path(outdir, files_shortname[ii])
          fst::write_fst(chunk, out_chunk_name, compress)
          return(files_shortname)
        }
        NULL # return NULL
      }, ...)
      return(disk.frame(outdir))
    } else {
      # TODO really inefficient
      shard(diskf,
            outdir = outdir,
            nchunks = nchunks,
            overwrite = TRUE,
            shardby = shardby,
            compress = compress,
            shardby_function=shardby_function, 
            sort_splits=sort_splits, desc_vars=desc_vars,
            ...
            )
    }
  } else if ("data.frame" %in% class(diskf)) {
    if(".out.disk.frame.id" %in% names(diskf)) {
      diskf[,{
        if (base::nrow(.SD) > 0) {
          list_columns = purrr::map_lgl(.SD, is.list)
          if(any(list_columns)){
            stop(glue::glue("The data frame contains these list-columns: '{paste0(names(.SD)[list_columns], collapse='\', \'')}'. List-columns are not yet supported by disk.frame. Remove these columns to create a disk.frame"))
          } else {
            fst::write_fst(.SD, file.path(outdir, paste0(.BY, ".fst")), compress = compress)
            NULL
          }
        }
        NULL
      }, .out.disk.frame.id]
      res = disk.frame(outdir)
      add_meta(res, shardkey = shardby, shardchunks = nchunks, compress = compress)
    } else {
      as.disk.frame(diskf, outdir = outdir, nchunks = nchunks, overwrite = TRUE, shardby = shardby, compress = compress, ...)
    }
  } else {
    stop("write_disk.frame error: diskf must be a disk.frame or data.frame")
  }
}

#' @rdname write_disk.frame
output_disk.frame <- function(...) {
  warning("output_disk.frame is DEPRECATED. Use write_disk.frame istead")
  write_disk.frame(...)
}
