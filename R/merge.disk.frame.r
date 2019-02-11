#' Merge function for disk.frames
#' @export
#' @param x a disk.frame
#' @param y a disk.frame or data.frame
#' @param outdir The output directory for the disk.frame
#' @param merge_by_chunk_id if TRUE then only chunks in df1 and df2 with the same chunk id will get merged
#' @param ... passed to merge and map.disk.frame
#' @importFrom data.table data.table setDT
#' @import dtplyr
merge.disk.frame <- function(x, y, outdir, ..., merge_by_chunk_id = F) {  
  fs::dir_create(outdir)
  stopifnot("disk.frame" %in% class(x))
  
  if("data.frame" %in% class(y)) {
    map.disk.frame(x, function(df1) merge(x, y, ...), ...)
  } else if (merge_by_chunk_id | (all(shardkey(x) == shardkey(y)))) {
    # ifthe shardkeys are the same then only need to match by segment id
    # as account with the same shardkey must end up in the same segment
    path1 = attr(x,"path")
    path2 = attr(y,"path")
    
    df3 = merge(
      data.table(
        chunk_id = list.files(path1), 
        pathA = list.files(path1,full.names = T)
      ),
      data.table(
        chunk_id = list.files(path2), 
        pathB = list.files(path2,full.names = T)
      )
    )
    setDT(df3)
    df3[,{
      data1 = read_fst(pathA,as.data.table = T)
      data2 = read_fst(pathB,as.data.table = T)
      data3 = merge(data1, data2, ...)
      rm(data1); rm(data2); gc()
      write_fst(data3, glue("{outdir}/{.BY}"))
      NULL
    }, chunk_id]
    return(disk.frame(outdir))
  } else {
    stop("Cartesian joins are currently not implemented. Either make y a data.frame or set merge_by_chunk_id to TRUE")
    
    # have to make every possible combination
    # path1 = attr(df1,"path")
    # path2 = attr(df2,"path")
    # 
    # df3 = merge(
    #   data.table(
    #     justmerge = T,
    #     chunk_id1 = list.files(path1), 
    #     pathA = list.files(path1,full.names = T)
    #   ),
    #   data.table(
    #     justmerge = T,
    #     chunk_id2 = list.files(path2), 
    #     pathB = list.files(path2,full.names = T)
    #   ),
    #   by = "justmerge",
    #   all=T,
    #   allow.cartesian = T
    # )
    # 
    # 
    # setDT(df3)
    # i <- 0
    # mapply(function(pathA, pathB) {
    #   stop("error")
    #   data1 = read_fst(pathA,as.data.table = T, columns = c("ACCOUNT_ID","MONTH_KEY"))
    #   data2 = read_fst(pathB,as.data.table = T, columns = c("ACCOUNT_ID","MONTH_KEY"))
    #   data3 = merge(data1, data2, ...)
    #   rm(data1); rm(data2); gc()
    #   if(nrow(data3) > 0) {
    #     i <<- i + 1
    #     write_fst(data3, glue("{outdir}/{i}.fst"))
    #   }
    #   NULL
    # },df3$pathA, df3$pathB)
    # return(disk.frame(outdir))
  }
}