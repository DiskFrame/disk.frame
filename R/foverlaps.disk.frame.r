#' Apply data.table's foverlaps to the disk.frame
#' @description EXPERIMENTAL
#' @param df1 A disk.frame
#' @param df2 A disk.frame or a data.frame
#' @param by.x character/string vector. by.x used in foverlaps
#' @param by.y character/string vector. by.x used in foverlaps
#' @param outdir The output directory of the disk.frame
#' @param merge_by_chunk_id If TRUE then the merges will happen for chunks in df1 and df2 with the same chunk id which speed up processing. Otherwise every chunk of df1 is merged with every chunk of df2. Ignored with df2 is not a disk.frame
#' @param compress The compression ratio for fst
#' @param overwrite overwrite existing directory
#' @param ... passed to data.table::foverlaps and disk.frame::map.disk.frame
#' @import fst
#' @importFrom glue glue
#' @importFrom data.table foverlaps data.table setDT setkeyv
#' @importFrom future.apply future_lapply
#' @importFrom pryr do_call
#' @export
#' @examples
#' library(data.table)
#' 
#' ## simple example:
#' x = as.disk.frame(data.table(start=c(5,31,22,16), end=c(8,50,25,18), val2 = 7:10))
#' y = as.disk.frame(data.table(start=c(10, 20, 30), end=c(15, 35, 45), val1 = 1:3))
#' byxy = c("val1", "start", "end")
#' xy.df = foverlaps.disk.frame(
#'   x, y, by.x = byxy, by.y = byxy, 
#'   merge_by_chunk_id = TRUE, overwrite = TRUE)
#' 
#' # clean up
#' delete(x)
#' delete(y)
#' delete(xy.df)
foverlaps.disk.frame <- function(df1, df2, by.x = if (identical(shardkey(df1)$shardkey, "")) shardkey(df1)$shardkey else shardkey(df2)$shardkey, by.y = shardkey(df2)$shardkey, ...,outdir = tempfile("df_foverlaps_tmp", fileext = ".df"), merge_by_chunk_id = FALSE, compress=50, overwrite = TRUE) {

  stopifnot("disk.frame" %in% class(df1))
  
  overwrite_check(outdir, overwrite)
  
  if("data.frame" %in% class(df2)) {
    map.disk.frame(df1, ~foverlaps(.x, df2, ...), ..., lazy = FALSE, compress = compress, overwrite = overwrite)
  } else if (merge_by_chunk_id | (identical(shardkey(df1), shardkey(df2)))) {
    # if the shardkeys are the same then only need to match by segment id
    # as account with the same shardkey must end up in the same segment
    path1 = attr(df1,"path")
    path2 = attr(df2,"path")
    
    df3 = merge(
      data.table(
        chunk_id = list.files(path1), 
        pathA = list.files(path1,full.names = TRUE),
        file_id = 1:length(list.files(path1))
      ),
      data.table(
        chunk_id = list.files(path2), 
        pathB = list.files(path2,full.names = TRUE)
      ),
      by = "chunk_id"
    )
    setDT(df3)
    
    dotdotdot = list(...)
    
    furrr::future_map(1:nrow(df3), function(row) {
    #future.apply::future_lapply(1:nrow(df3), function(row) {
    #lapply(1:nrow(df3), function(row) {
      chunk_id = df3[row, chunk_id]
      
      data1 = get_chunk(df1, chunk_id)
      data2 = get_chunk(df2, chunk_id)
      
      setDT(data1)
      setDT(data2)
      
      setkeyv(data2, by.y[(length(by.y)-2+1):length(by.y)])
      
      dotdotdot$x = data1
      dotdotdot$y = data2
      data3 = pryr::do_call(foverlaps, dotdotdot)
      rm(data1); rm(data2); gc()
      outdir
      fst::write_fst(data3, glue::glue("{outdir}/{chunk_id}"), compress = compress)
      rm(data3); gc()
      NULL
    })
    return(disk.frame(outdir))
  } else {
    stop("foverlaps.disk.frame: only merge_by_chunk_id = TRUE is implemented")
  }
}
