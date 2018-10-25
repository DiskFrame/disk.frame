#' @export
#' @import fs
#' @import fst
names.disk.frame <- function(df) {
  res = attr(df,"path") %>% 
    fs::dir_ls(type="file")
  fst::metadata_fst(res[1])$columnNames
}

#' @export
colnames.disk.frame <- function(df) names.disk.frame(df)