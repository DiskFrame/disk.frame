rbindlist.disk.frame <- function(df_list, outdir, by_chunk_id = T, parallel = T, compress=50) {
  #browser()
  if(!dir.exists(outdir)) dir.create(outdir)
  if(by_chunk_id) {
    list_of_paths = map_chr(df_list, ~attr(.x,"path"))
    list_of_chunks = map_dfr(list_of_paths, ~data.table(path = dir(.x),full_path = dir(.x,full.names = T)))
    setDT(list_of_chunks)
    
    # split the list of chunks into lists for easy operation with future
    slist = split(list_of_chunks$full_path,list_of_chunks$path)
    
    if(parallel) {
      system.time(future_lapply(1:length(slist), function(i) {
        full_paths1 = slist[[i]]
        outfilename = names(slist[i])
        write_fst(map_dfr(full_paths1, ~read_fst(.x)),file.path(outdir,outfilename), compress = compress)
        NULL
      }))
    } else {
      system.time(lapply(1:length(slist), function(i) {
        full_paths1 = slist[[i]]
        outfilename = names(slist[i])
        write_fst(map_dfr(full_paths1, ~read_fst(.x)),file.path(outdir,outfilename), compress = compress)
        NULL
      }))
    }
    
    # list_of_chunks[,{
    #   write_fst(map_dfr(full_path, ~read_fst(.x)),file.path(outdir,.BY))
    #   NULL
    #   }, path]
    # 
    return(disk.frame(outdir))
  } else {
    stop("not implemented yet")
  }
  
}
