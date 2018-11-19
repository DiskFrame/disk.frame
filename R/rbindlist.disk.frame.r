#' rbindlist disk.frames together
#' @param df_list A list of disk.frames
#' @param outdir Output directory of the row-bound disk.frames
#' @param by_chunk_id If TRUE then only the chunks with the same chunk IDs will be bound
#' @param parallel if TRUE then bind multiple disk.frame simultaneously, Defaults to TRUE
#' @param compress 0-100, 100 being the highest compression rate.
#' @import data.table purrr future future.apply fs
#' @export
rbindlist.disk.frame <- function(df_list, outdir, by_chunk_id = T, parallel = T, compress=50, overwrite = T) {
  overwrite_check(outdir, overwrite)
  
  
  if(by_chunk_id) {
    list_of_paths = purrr::map_chr(df_list, ~attr(.x,"path"))
    list_of_chunks = purrr::map_dfr(list_of_paths, ~data.table(path = list.files(.x),full_path = list.files(.x,full.names = T)))
    setDT(list_of_chunks)
    
    # split the list of chunks into lists for easy operation with future
    slist = split(list_of_chunks$full_path,list_of_chunks$path)
    
    if(parallel) {
      system.time(future_lapply(1:length(slist), function(i) {
        full_paths1 = slist[[i]]
        outfilename = names(slist[i])
        fst::write_fst(purrr::map_dfr(full_paths1, ~read_fst(.x)),file.path(outdir,outfilename), compress = compress)
        NULL
      }))
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
    #browser()
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