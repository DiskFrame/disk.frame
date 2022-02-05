#' A function to convert a disk.frame to parquet format
#' @importFrom arrow write_parquet
#' @export
disk.frame_to_parquet <- function(df, outdir) {
  if("disk.frame" %in% class(df)) {
    path = attr(df, "path")
  } else {
    path = df
  }
  
  path = normalizePath(path)
  fst_files = normalizePath(list.files(path, "fst$", full.names = TRUE, recursive=TRUE))
  
  if(!fs::dir_exists(outdir)) {
    fs::dir_create(outdir)
  }
  
  
  future.apply::future_lapply(fst_files, function(fst_file) {
    file_name = basename(fst_file)
    file_name = paste0(stringr::str_sub(file_name, 1, nchar(file_name)-4), ".parquet")
    path_name = normalizePath(dirname(fst_file))
    
    # remove base directory from path
    path_name = stringr::str_sub(path_name, nchar(path)+1)
    
    if(!fs::dir_exists(file.path(outdir, path_name))) {
      fs::dir_create(file.path(outdir, path_name))
    }
    
    arrow::write_parquet(fst::read_fst(fst_file), file.path(outdir, path_name, file_name))
  })
}