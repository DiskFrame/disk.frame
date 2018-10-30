#' Create a disk.frame
#' @param path The path to store the output file or to a directory
#' @param backend The only available backend is fst at the moment
#' @export
disk.frame <- function(path, ..., backend = "fst") {
  if(dir.exists(path)) {
    disk.frame_folder(path)
  } else if (file.exists(path)) {
    disk.frame_fst(path)
  }
}

#' Add metadata to the disk.frame
#' @import fs jsonlite
#' @export
add_meta <- function(df, ..., nchunks = nchunks.disk.frame(df), shardkey = "", shardchunks = -1) {
  stopifnot("disk.frame" %in% class(df))
  #\\
  # create the metadata folder if not present
  fs::dir_create(file.path(attr(df,"path"),".metadata"))
  json_path = fs::file_create(file.path(attr(df,"path"),".metadata", "meta.json"))
  
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

#' Create a data frame pointed to a folder
disk.frame_folder <- function(path, ....) {
  df <- list()
  df$files <- list.files(path, full.names = T)
  df$files_short <- list.files(path)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- c("disk.frame", "disk.frame.folder")
  #attr(df, "metadata") <- sapply(files,function(file1) fst::fst.metadata(file1))
  attr(df, "performing") <- "none"
  df
}


#' Create a disk.frame from fst files
#' @param path The path to store the output file or to a directory
#' @import fst
disk.frame_fst <- function(path, ...) {
  df <- list()
  attr(df, "metadata") <- fst::fst.metadata(path)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- c("disk.frame", "disk.frame.file")
  attr(df, "performing") <- "none"
  df
}


prepare_dir.disk.frame <- function(df, path, clean = F) {
  fpath = attr(df, "path")
  fpath2 = file.path(fpath,path)
  if(!dir.exists(fpath2)) {
    dir.create(fpath2)
  } else if(clean) {    
    sapply(list.files(fpath2,full.names = T), unlink, recursive =T, force  = T)
  }
  fpath2
}


#' is the disk.frame ready
is_ready <- function(df) {
  return(TRUE)
  UseMethod("is_ready")  
}

status <- function(...) {
  UseMethod("status")
}

status.disk.frame <- function(df) {
  #list.files(
  perf = attr(df,"performing")
  if(perf == "none") {
    nc = nchunk(df, skip.ready.check = T)
    return(list(status = "at rest", nchunk = nc, nchunk_ready = nc))
  } else if (perf == "hard_group_by") {
    fpath = attr(df, "parent")
    ndf = nchunk(df, skip.ready.check = T)
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
    return(T)
  } else {
    return(T)
  }
}

#' Checks if the df is a single-file based disk.frame
is.file.disk.frame <- function(df, check.consistency = T) {
  if(check.consistency) {
    fpath <- attr(df,"path")
    if(!dir.exists(fpath) & file.exists(fpath)) {
      return(TRUE) 
    } else {
      return(F)
    }
  }
  return("disk.frame.file" %in% class(df))
}

#' @rdname is.file.disk.frame
is.dir.disk.frame <- function(...) {
  !is.file.disk.frame(...)
}

#' Head of the disk.frame
#' @export
#' @import fst
head.disk.frame <- function(df, n = 6L, ...) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  cmds <- attr(df, "lazyfn")
  if(dir.exists(path1)) {
    path2 <- list.files(path1,full.names = T)[1]
    head(disk.frame:::play(fst::read.fst(path2, from = 1, to = n), cmds), n = n, ...)
  } else {
    head(disk.frame:::play(fst::read.fst(path1, from = 1, to = n), cmds), n = n, ...)
  }
}

#' tail of disk.frame
#' @export
#' @import fst
tail.disk.frame <- function(df, n = 6L, ...) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- list.files(path1,full.names = T)
    path2 <- path2[length(path2)]
    tail(fst::read.fst(path2, from = 1, to = n), n = n, ...)
  } else {
    stop("tail failed: dir not exist")
  }
}

#' Number of rows of disk.frame
#' @export
nrow <- function(...) {
  UseMethod("nrow")
}

#' @rdname nrow
#' @export
nrow.default <- function(...) {
  base::nrow(...)
}

#' @rdname nrow
#' @export
#' @param df a disk.frame
#' @import fst
nrow.disk.frame <- function(df) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- list.files(path1,full.names = T)
    tmpfstmeta = fst::fst.metadata(path2[1])
    if("nrOfRows" %in% names(tmpfstmeta)) {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$nrOfRows)))
    } else {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$NrOfRows)))
    }
  } else {
    #return(fst::fst.metadata(path1)$NrOfRows)
    stop("nrow error")
  }
}

#' do to all chunks
#' @export
#' @rdname map
chunk_lapply <- function (...) {
  warning("chunk_lapply is deprecated in favour of map.disk.frame")
  map.disk.frame(...)
}

#' Apply the same function to all chunks
#' @param df a disk.frame
#' @param fn a function to apply to each of the chunks
#' @param outdir the output directory
#' @param keep the columns to keep from the input
#' @param chunks The number of chunks to output
#' @param lazy if TRUE then do this lazily
#' @param compress 0-100 fst compression ratio
#' @import fst
#' @import future
#' @import future.apply
#' @import purrr
#' @export
map.disk.frame <- function(df, fn, ..., outdir = NULL, keep=NULL, chunks = nchunks(df), compress = 50, lazy = T) {
  #list.files(
  fn = purrr::as_mapper(fn)
  if(lazy) {
    attr(df, "lazyfn") = c(attr(df, "lazyfn"), fn)
    return(df)
  }
  
  if(!is.null(outdir)) {
    fs::dir_create(outdir)
  }
  
  stopifnot(is_ready(df))
  keep1 = attr(df,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(df, "path")
  files <- list.files(path, full.names = T)
  files_shortname <- list.files(path)
  #list.files(
  res = future.apply::future_lapply(1:length(files), function(ii) {
    #res = fn(read_fst(files[ii], as.data.table=T, columns=keep), ...)
    #list.files(
    ds = get_chunk.disk.frame(df, ii, keep=keep)
    res = fn(ds)
    if(!is.null(outdir)) {
      fs::dir_create(outdir)
      write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(ii)
    } else {
      return(res)
    }
  })
  #list.files(
  if(!is.null(outdir)) {
    if(!dir.exists(outdir)) dir.create(outdir)
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}

#' Lazy chunk_lapply wrapper
#' @export
#' @rdname map
lazy <- function(df,...) {
  UseMethod("lazy")
}

lazy.disk.frame <- function(df, fn, ...) {
  map.disk.frame(df, fn, lazy=T, ...)
}

#' Lazy chunk_lapply wrapper
#' @export
#' @rdname map
delayed <- function(...) {
  UseMethod("delayed")
}

#' @export
#' @rdname map
delayed.disk.frame <- function(df, fn, ...) {
  map.disk.frame(df, fn, lazy = T, ...)
}
 
#' [ interface for disk.frame using fst backend
#' @import data.table future fst future.apply
#' @export
`[.disk.frame` <- function(df, i, j,..., keep = NULL) {
  res <- NULL
  fpath <- attr(df,"path")
  
  ff <- list.files(attr(df,"path"))
  
  i = deparse(substitute(i))
  j = deparse(substitute(j))
  dotdot = deparse(substitute(...))
  
  res <- future.apply::future_lapply(ff, function(k,i,j,dotdot) {
    # sometimes the i and j an dotdot comes in the form of a vector so need to paste them together
    j = paste0(j,collapse="")
    dotdot = paste0(dotdot,collapse="")
    i = paste0(i,collapse="")

    if(dotdot == "NULL") {
      code = sprintf("a[%s,%s]", i, j)
    } else if (j == "NULL") {
      code = sprintf("a[%s]", i)
    } else {
      code = sprintf("a[%s,%s,%s]", i, j, dotdot)
    }
    a = get_chunk.disk.frame(df, k)
    
    aa <- eval(parse(text=code))
    #list.files(
    rm(a); gc()
    aa
  }, i, j, dotdot)
  
  # sometimes the returned thing is a vetor e.g. df[,.N]
  if("data.frame" %in% class(res[[1]])) {
    return(rbindlist(res))
  } else if(is.vector(res)) {
    return(unlist(res, recursive = F))
  } else {
    warning("spooky")
    return(res)
  }
}

#' Distribute (chunk-up/break-up) a fst file into chunks into a folder; an alias for shard
#' @export
#' @rdname shard
distribute <- function(...) {
  UseMethod("shard")
}

#' Number of columns
#' @import fst
#' @export
ncol <- function(x) {
  UseMethod("ncol")
}

#' @import fs
#' @export
#' @rdname ncol
ncol.disk.frame <- function(df) {
  length(colnames(df))
}

#' @export
ncol.default <- function(x) {
  base::ncol(x)
}
