#' Create a disk.frame from a folder
#' @param path The path to store the output file or to a directory
#' @param backend The only available backend is fst at the moment
#' @export
#' @examples
#' path = file.path(tempdir(),"cars")
#' as.disk.frame(cars, outdir=path, overwrite = TRUE, nchunks = 2)
#' df = disk.frame(path)
#' head(df)
#' nchunks(df)
#' # clean up
#' delete(df)
disk.frame <- function(path, backend = "fst") {
  
  # only fst backend is implemented at the moment
  stopifnot(backend == "fst")
  
  if(dir.exists(path)) {
    disk.frame_folder(path)
  } else if (file.exists(path)) {
    disk.frame_fst(path)
  } else {
    # if neither exists then create it
    fs::dir_create(path)
    disk.frame(path)
  }
}

#' Add metadata to the disk.frame
#' 
#' This is the only mechanism to add metadata to a disk.frame at the moment.
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom fs dir_create file_create
#' @param df a disk.frame
#' @param nchunks number of chunks
#' @param shardkey the shard key
#' @param shardchunks The number of chunks to shard to. Sometimes the number of actual file chunks is different to the number of intended chunks. In this case the shardchunks is the intended number
#' @param ... any other metadata the user wishes to keep.
#' @noRd
#' @noMd
# @export
#' @examples
#' #cars.df = as.disk.frame(cars)
#' 
#' # adding some useless metadata to the repo
#' #add_meta(cars.df, useless = TRUE)
#' 
#' # clean up
#' #delete(cars.df)
add_meta <- function(df, ..., nchunks = nchunks.disk.frame(df), shardkey = "", shardchunks = -1) {
  stopifnot("disk.frame" %in% class(df))
  
  if(is.null(shardkey)) {
    shardkey = ""
  }

  # create the metadata folder if not present
  fs::dir_create(file.path(attr(df,"path"), ".metadata"))
  json_path = fs::file_create(file.path(attr(df,"path"), ".metadata", "meta.json"))
  
  filesize = file.size("meta.json")
  meta_out = NULL
  if(is.na(filesize)) {
    # the file is empty
    meta_out = jsonlite::toJSON(
        c(
          list(
            nchunks = nchunks, 
            shardkey = shardkey, 
            shardchunks = shardchunks), 
          list(...)
        )
      )
  } else {
    meta_out = jsonlite::fromJSON(json_path)
    meta_out$nchunks = nchunks
    meta_out$shardkey = shardkey
    meta_out$shardchunks = shardchunks
    meta_out <- c(meta_out, list(...))
  }
  cat(meta_out, file = json_path)
  df
}

#' Create a disk.frame pointed to a folder
#' @rdname disk.frame_fst
#' @noRd
disk.frame_folder <- function(path) {
  df <- list()
  df$files <- list.files(path, full.names = TRUE)
  df$files_short <- list.files(path)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- c("disk.frame", "disk.frame.folder")
  #attr(df, "metadata") <- sapply(files,function(file1) fst::fst.metadata(file1))
  attr(df, "performing") <- "none"
  df
}


#' @param path The path to store the output file or to a directory
#' @import fst
#' @noRd
disk.frame_fst <- function(path) {
  df <- list()
  attr(df, "metadata") <- fst::fst.metadata(path)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- c("disk.frame", "disk.frame.file")
  attr(df, "performing") <- "none"
  df
}

prepare_dir.disk.frame <- function(df, path, clean = FALSE) {
  fpath = attr(df, "path")
  fpath2 = file.path(fpath,path)
  if(!dir.exists(fpath2)) {
    dir.create(fpath2)
  } else if(clean) {    
    sapply(list.files(fpath2, full.names = TRUE), unlink, recursive = TRUE, force  = TRUE)
  }
  fpath2
}

#' The readiness of a disk.frame
#' @description 
#' Sometimes a disk.frame may be in use and this check whether it is ready from a long running non-blocking process.
# TODO 
#' @param df a disk.frame
#' @noRd
is_ready <- function(df) {
  return(TRUE)
  UseMethod("is_ready")  
}

status <- function(...) {
  UseMethod("status")
}

status.disk.frame <- function(df) {
  perf = attr(df,"performing")
  if(perf == "none") {
    nc = nchunk(df, skip.ready.check = TRUE)
    return(list(status = "at rest", nchunk = nc, nchunk_ready = nc))
  } else if (perf == "hard_group_by") {
    fpath = attr(df, "parent")
    ndf = nchunk(df, skip.ready.check = TRUE)
    if(!dir.exists(file.path(fpath, ".performing"))) {
      return(list(status = "hard group by", nchunk = ndf, nchunk_ready = 0))
    } else if(dir.exists(file.path(fpath, ".performing", "outchunks"))) {
      l = length(list.files(file.path(fpath, ".performing", "outchunks")))
      if(l == ndf) {
        attr(df, "performing") <- "none"
        return(list(status ="none", nchunk = ndf, nchunk_read = ndf))
      }
      return(list(status = "hard group by", nchunk = ndf, nchunk_ready = l))
    }
  } else {
    return(list(status = "unknown", nchunk = NA, nchunk_ready = NA))
  }
}

is_ready.disk.frame <- function(df) {
  sts = status(df)
  if(sts$status == "none") {
    return(TRUE)
  } else {
    return(TRUE)
  }
}

#' Is the disk.frame a single-file or a folder
#' @description 
#' Checks if the df is a single-file based disk.frame
#' @param df a disk.frame
#' @param check.consistency check for consistency e.g. if it's actually a file
#' @noRd
is.file.disk.frame <- function(df, check.consistency = TRUE) {
  if(check.consistency) {
    fpath <- attr(df,"path")
    if(!dir.exists(fpath) & file.exists(fpath)) {
      return(TRUE) 
    } else {
      return(FALSE)
    }
  }
  return("disk.frame.file" %in% class(df))
}

#' @rdname is.file.disk.frame
#' @noRd
is.dir.disk.frame <- function(df, check.consistency = TRUE) {
  !is.file.disk.frame(df, check.consistency = check.consistency)
}

#' Head and tail of the disk.frame
#' @param x a disk.frame
#' @param n number of rows to include
#' @param ... passed to base::head or base::tail
#' @export
#' @import fst
#' @importFrom utils head
#' @importFrom glue glue
#' @importFrom fs dir_exists
#' @rdname head_tail
#' @examples 
#' cars.df = as.disk.frame(cars)
#' head(cars.df)
#' tail(cars.df)
#'
#' # clean up 
#' delete(cars.df)
head.disk.frame <- function(x, n = 6L, ...) {
  stopifnot(is_ready(x))
  path1 <- attr(x,"path")
  cmds <- attr(x, "recordings")
  if(fs::dir_exists(path1)) {
    path2 <- list.files(path1,full.names = TRUE)[1]
    head(play(fst::read_fst(path2, from = 1, to = n, as.data.table = TRUE), cmds), n = n, ...)
  } else {
    stop(glue::glue("dir {path1} does not exist"))
  }
}

#' @export
#' @import fst
#' @importFrom utils tail
#' @rdname head_tail
tail.disk.frame <- function(x, n = 6L, ...) {
  stopifnot(is_ready(x))
  path1 <- attr(x,"path")
  cmds <- attr(x, "recordings")
  if(dir.exists(path1)) {
    path2 <- list.files(path1,full.names = TRUE)
    path2 <- path2[length(path2)]
    tail(play(fst::read_fst(path2, as.data.table = TRUE), cmds), n = n, ...)
  } else {
    stop(glue::glue("dir {path1} does not exist"))
  }
}
