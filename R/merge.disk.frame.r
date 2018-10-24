shardkey <- function(...) {
  #"not implemented yet"
  #runif(1)
  T
}

keep.disk.frame <- function(df, selections) {
  stopifnot("disk.frame" %in% class(df))
  
  df1 = disk.frame(attr(df,"path"))
  attr(df1,"keep") = selections
  df1
}

merge.disk.frame <- function(df1, df2, outdir, ..., merge_by_chunk_id = F) {
  
  if(!dir.exists(outdir)) dir.create(outdir)
  stopifnot("disk.frame" %in% class(df1))
  
  if("data.frame" %in% class(df2)) {
    chunk_lapply(df1, function(df1) merge(df1, df2, ...), ...)
  } else if (merge_by_chunk_id | (all(sort(shardkey(df1)) == sort(shardkey(df2))))) {
    # ifthe shardkeys are the same then only need to match by segment id
    # as account with the same shardkey must end up in the same segment
    path1 = attr(df1,"path")
    path2 = attr(df2,"path")
    
    df3 = merge(
      data.table(
        chunk_id = dir(path1), 
        pathA = dir(path1,full.names = T)
      ),
      data.table(
        chunk_id = dir(path2), 
        pathB = dir(path2,full.names = T)
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
    # have to make every possible combination
    path1 = attr(df1,"path")
    path2 = attr(df2,"path")
    
    df3 = merge(
      data.table(
        justmerge = T,
        chunk_id1 = dir(path1), 
        pathA = dir(path1,full.names = T)
      ),
      data.table(
        justmerge = T,
        chunk_id2 = dir(path2), 
        pathB = dir(path2,full.names = T)
      ),
      by = "justmerge",
      all=T,
      allow.cartesian = T
    )
    stop("error not implemented")
    setDT(df3)
    i <- 0
    mapply(function(pathA, pathB) {
      data1 = read_fst(pathA,as.data.table = T, columns = c("ACCOUNT_ID","MONTH_KEY"))
      data2 = read_fst(pathB,as.data.table = T, columns = c("ACCOUNT_ID","MONTH_KEY"))
      data3 = merge(data1, data2, ...)
      rm(data1); rm(data2); gc()
      if(nrow(data3) > 0) {
        i <<- i + 1
        write_fst(data3, glue("{outdir}/{i}.fst"))
      }
      NULL
    },df3$pathA, df3$pathB)
    return(disk.frame(outdir))
  }
}