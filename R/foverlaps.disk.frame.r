#' Apply data.table's foverlaps to the disk.frame
#' @param df1 A disk.frame
#' @param df2 A disk.frame or a data.frame
#' @param outdir The output directory of the disk.frame
#' @param merge_by_chunk_id If TRUE then the merges will happen for chunks in df1 and df2 with the same chunk id which speed up processing. Otherwise everychunk of df1 is merged with every chunk of df2. Ignored with df2 is not a disk.frame
#' @param compress The compression ratio for fst
#' @import fst
#' @importFrom glue glue
#' @importFrom data.table foverlaps data.table setDT
#' @importFrom future.apply future_lapply
#' @export
foverlaps.disk.frame <- function(df1, df2, outdir, ..., merge_by_chunk_id = F, compress=50, overwrite = T) {
  stopifnot("disk.frame" %in% class(df1))
  
  overwrite_check(outdir, overwrite)
  
  if("data.frame" %in% class(df2)) {
    map.disk.frame(df1, function(df1) foverlaps(df1, df2, ...), ..., compress = compress, overwrite = overwrite)
  } else if (merge_by_chunk_id | (identical(shardkey(df1), shardkey(df2)))) {
    # if the shardkeys are the same then only need to match by segment id
    # as account with the same shardkey must end up in the same segment
    path1 = attr(df1,"path")
    path2 = attr(df2,"path")
    
    df3 = merge(
      data.table(
        chunk_id = list.files(path1), 
        pathA = list.files(path1,full.names = T),
        file_id = 1:length(list.files(path1))
      ),
      data.table(
        chunk_id = list.files(path2), 
        pathB = list.files(path2,full.names = T)
      ),
      by = "chunk_id"
    )
    setDT(df3)
    
    dotdotdot = list(...)
    
    future.apply::future_lapply(1:nrow(df3), function(row) {
      chunk_id = df3[row,chunk_id]
      
      data1 = get_chunk.disk.frame(df1,chunk_id)
      data2 = get_chunk.disk.frame(df2,chunk_id)
      dotdotdot$x = data1
      dotdotdot$y = data2
      data3 = do_call(foverlaps, dotdotdot)
      rm(data1); rm(data2); #gc()
      outdir
      write_fst(data3, glue("{outdir}/{chunk_id}"), compress = compress)
      rm(data3); #gc()
      NULL
    })
    return(disk.frame(outdir))
  } else {
    stop("this foverlaps.disk.frame branch is not implemented")
  }
}
