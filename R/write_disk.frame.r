#' Write disk.frame to disk
#' @description
#' Write a data.frame/disk.frame to a disk.frame location. If df is a data.frame
#' then using the as.disk.frame function is recommended for most cases
#' @param diskf a disk.frame
#' @param outdir output directory for the disk.frame
#' @param nchunks number of chunks
#' @param overwrite overwrite output directory
#' @param shardby the columns to shard by
#' @param partitionby the columns to (folder) partition by
#' @param compress compression ratio for fst files
#' @param ... passed to cmap.disk.frame
#' @export
#' @import fst fs
#' @importFrom dplyr group_map
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
  shardby=NULL, 
  partitionby=NULL,
  compress = 50, ...) {

  force(nchunks)
  overwrite_check(outdir, overwrite)

  if(is.null(outdir)) {
    stop("write_disk.frame error: outdir must not be NULL")
  }

  if(is_disk.frame(diskf)) {
    if(!is.null(partitionby)) {
      
      # for each chunk group by the partionby and then write out a partitioned disk.frame for each chunk
      list_of_paths = diskf %>%
        cimap(~{
          tmp_dir_to_write = tempfile(as.character(.y))
          tmp = .x %>%
            group_by(!!!syms(partitionby)) %>%
            dplyr::group_map(~{
              # convert group keys to path
              tmp_path = lapply(names(.y), function(n) {
                sprintf("%s=%s", n, .y[, n])
              }) %>%
                do.call(file.path, .)
              
              final_tmp_path = file.path(tmp_dir_to_write, tmp_path)
              as.disk.frame(.x, final_tmp_path, overwrite = FALSE)
            })
          return(tmp_dir_to_write)
        }, lazy=FALSE)
      
      # for each of the chunks, do a soft row-append
      partitioned_files = lapply(list_of_paths, function(path) {
        # each path is a partitioned disk.frame
        files = list.files(path, full.names = TRUE, recursive=TRUE)
        tmp = data.frame(partition_path =  files %>% 
          dirname %>% 
          sapply(tools::file_path_as_absolute) %>% 
          stringr::str_sub(nchar(path)+2))
        tmp = tmp %>% mutate(path=path, files=files)
        
        tmp
      }) %>% rbindlist
      
      partitioned_files %>% 
        group_by(partition_path) %>% 
        dplyr::group_map(function(df, grp) {
          mapply(function(file, i) {
            outfile = file.path(outdir, grp$partition_path, paste0(i, ".fst"))
            if(!dir.exists(file.path(outdir, grp$partition_path))) {
              fs::dir_create(file.path(outdir, grp$partition_path))
            }
            fs::file_move(file, outfile)
          }, df$files, seq_along(df$files))
        })
        
      return(disk.frame(outdir))
        
    } else if(is.null(shardby)) {
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
        return(NULL)
      }, ..., future.seed = TRUE)
      return(disk.frame(outdir))
    } else {
      # TODO really inefficient
      shard(diskf,
            outdir = outdir,
            nchunks = nchunks,
            overwrite = TRUE,
            shardby = shardby,
            compress = compress,
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
