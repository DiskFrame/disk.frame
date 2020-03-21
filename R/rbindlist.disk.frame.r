#' rbindlist disk.frames together
#' @param df_list A list of disk.frames
#' @param outdir Output directory of the row-bound disk.frames
#' @param by_chunk_id If TRUE then only the chunks with the same chunk IDs will be bound
#' @param parallel if TRUE then bind multiple disk.frame simultaneously, Defaults to TRUE
#' @param compress 0-100, 100 being the highest compression rate.
#' @param overwrite overwrite the output directory
#' @param .progress A logical, for whether or not to print a progress bar for multiprocess, multisession, and multicore plans. From {furrr}
#' @import fs
#' @importFrom data.table data.table setDT
#' @importFrom future.apply future_lapply
#' @importFrom purrr map_chr map_dfr map map_lgl
#' @importFrom purrr map
#' @export
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # row-bind two disk.frames
#' cars2.df = rbindlist.disk.frame(list(cars.df, cars.df))
#' 
#' # clean up cars.df
#' delete(cars.df)
#' delete(cars2.df)
rbindlist.disk.frame <- function(df_list, outdir = tempfile(fileext = ".df"), by_chunk_id = TRUE, parallel = TRUE, compress=50, overwrite = TRUE, .progress = TRUE) {
  stopifnot(typeof(df_list) == "list")
  
  overwrite_check(outdir, overwrite)
  
  purrr::map(df_list, ~{
    if(!"disk.frame" %in% class(.x)) {
      stop("error running rbindlist.disk.frame: Not every element of df_list is a disk.frame")
    }
  })
  
  if(by_chunk_id) {
    list_of_paths = purrr::map_chr(df_list, ~attr(.x,"path", exact=TRUE))
    list_of_chunks = purrr::map_dfr(list_of_paths, ~data.table(path = list.files(.x),full_path = list.files(.x,full.names = TRUE)))
    setDT(list_of_chunks)
    
    # split the list of chunks into lists for easy operation with future
    slist = split(list_of_chunks$full_path,list_of_chunks$path)
    
    if(parallel) {
      #system.time(future.apply::future_lapply(1:length(slist), function(i) {
      if(.progress) {
        message("Appending disk.frames: ")
      }
      system.time(furrr::future_map(1:length(slist), function(i) {
        full_paths1 = slist[[i]]
        outfilename = names(slist[i])
        fst::write_fst(purrr::map_dfr(full_paths1, ~fst::read_fst(.x)),file.path(outdir,outfilename), compress = compress)
        NULL
      }, .progress = .progress))
    } else {
      system.time(lapply(1:length(slist), function(i) {
        full_paths1 = slist[[i]]
        outfilename = names(slist[i])
        fst::write_fst(purrr::map_dfr(full_paths1, ~fst::read_fst(.x)),file.path(outdir,outfilename), compress = compress)
        NULL
      }))
    }
    
    rbind_res = disk.frame(outdir)
    
    shardkeys <- purrr::map(df_list, shardkey)
    
    # if all the sharkeys are identical then
    ##browser
    if(all(purrr::map_lgl(shardkeys[-1], ~identical(.x, shardkeys[[1]])))) {
      return(add_meta(rbind_res, 
               shardkey = shardkeys[[1]]$shardkey,
               shardchunks = shardkeys[[1]]$shardchunks, 
               compress = compress))
    } else {
      return(rbind_res)
    }
  } else {
    stop("For rbindlist.disk.frame, only by_chunk_id = TRUE is implemented")
  }
}