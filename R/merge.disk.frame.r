#' Merge function for disk.frames
#' @export
#' @param x a disk.frame
#' @param y a disk.frame or data.frame
#' @param by the merge by keys
#' @param outdir The output directory for the disk.frame
#' @param merge_by_chunk_id if TRUE then only chunks in df1 and df2 with the same chunk id will get merged
#' @param overwrite overwrite the outdir or not
#' @param ... passed to merge and cmap.disk.frame
#' @importFrom data.table data.table setDT
#' @examples
#' b = as.disk.frame(data.frame(a = 51:150, b = 1:100))
#' d = as.disk.frame(data.frame(a = 151:250, b = 1:100))
#' bd.df = merge(b, d, by = "b", merge_by_chunk_id = TRUE)
#' 
#' # clean up cars.df
#' delete(b)
#' delete(d)
#' delete(bd.df)
merge.disk.frame <- function(x, y, by, outdir = tempfile(fileext = ".df"), ..., merge_by_chunk_id = FALSE, overwrite = FALSE) {  
  stopifnot("disk.frame" %in% class(x))
  overwrite_check(outdir, overwrite = TRUE)
  #fs::dir_create(outdir)
  
  if("data.frame" %in% class(y)) {
    yby = c(list(y=y, by=by), list(...))
    res = cmap(x, ~{
      res = do.call(merge, c(list(x = .x), yby))
      res
      }, outdir=outdir, ...)
    res  
  } else if (merge_by_chunk_id | shardkey_equal(shardkey(x), shardkey(y))) {
    # ifthe shardkeys are the same then only need to match by segment id
    # as account with the same shardkey must end up in the same segment
    path1 = attr(x,"path", exact=TRUE)
    path2 = attr(y,"path", exact=TRUE)
    
    df3 = merge(
      data.table(
        chunk_id = list.files(path1), 
        pathA = list.files(path1,full.names = TRUE)
      ),
      data.table(
        chunk_id = list.files(path2), 
        pathB = list.files(path2,full.names = TRUE)
      )
    )
    setDT(df3)
    df3[,{
      data1 = read_fst(pathA,as.data.table = TRUE)
      data2 = read_fst(pathB,as.data.table = TRUE)
      data3 = force(merge(data1, data2, by = by))
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
    #     justmerge = TRUE,
    #     chunk_id1 = list.files(path1), 
    #     pathA = list.files(path1,full.names = TRUE)
    #   ),
    #   data.table(
    #     justmerge = TRUE,
    #     chunk_id2 = list.files(path2), 
    #     pathB = list.files(path2,full.names = TRUE)
    #   ),
    #   by = "justmerge",
    #   all=TRUE,
    #   allow.cartesian = TRUE
    # )
    # 
    # 
    # setDT(df3)
    # i <- 0
    # mapply(function(pathA, pathB) {
    #   stop("error")
    #   data1 = read_fst(pathA,as.data.table = TRUE, columns = c("ACCOUNT_ID","MONTH_KEY"))
    #   data2 = read_fst(pathB,as.data.table = TRUE, columns = c("ACCOUNT_ID","MONTH_KEY"))
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
