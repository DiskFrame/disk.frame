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

#' Create a data frame pointed to a folder
disk.frame_folder <- function(path, ....) {
  df <- list()
  files <- dir(path, full.names = T)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- c("disk.frame", "disk.frame.folder")
  #attr(df, "metadata") <- sapply(files,function(file1) fst::fst.metadata(file1))
  attr(df, "performing") <- "none"
  df
}


#' Create a disk.frame from fst files
#' @path The path to store the output file or to a directory
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
    # file.remove(dir(fpath2,full.names = T))
    sapply(dir(fpath2,full.names = T), unlink, recursive =T, force  = T)
  }
  fpath2
}

#' Return the number of chunks
#' @export
nchunk <- function(...) {
  UseMethod("nchunk")
}

#' is the disk.frame ready
#' @export
is_ready <- function(df) {
  return(TRUE)
  UseMethod("is_ready")  
}

status <- function(...) {
  UseMethod("status")
}

status.disk.frame <- function(df) {
  #browser()
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
      l = length(dir(file.path(fpath, ".performing", "outchunks")))
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

#' @export
nchunk.disk.frame <- function(df, skip.ready.check = F) {
  if(!skip.ready.check) stopifnot(is_ready(df))
  fpath <- attr(df,"path")
  if(is.dir.disk.frame(df)) {
    return(length(dir(fpath)))
  } else {
    return(1)
  }
}

#' Checks if the df is a single-file based disk.frame
#' @export
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

#' Head
#' @export
head.disk.frame <- function(df, n = 6L, ...) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)[1]
    head(fst::read.fst(path2, from = 1, to = n), n = n, ...)
  } else {
    head(fst::read.fst(path1, from = 1, to = n), n = n, ...)
  }
}

#' tail of disk.frame
#' @export
tail.disk.frame <- function(df, n = 6L, ...) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)
    path2 <- path2[length(path2)]
    tail(fst::read.fst(path2, from = 1, to = n), n = n, ...)
  } else {
    tail(fst::read.fst(path1, from = (df$NrOfRows-n+1), to = df$NrOfRows), n = n, ...)
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
nrow.disk.frame <- function(df) {
  stopifnot(is_ready(df))
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)
    tmpfstmeta = fst::fst.metadata(path2[1])
    if("nrOfRows" %in% names(tmpfstmeta)) {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$nrOfRows)))
    } else {
      return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$NrOfRows)))
    }
  } else {
    return(fst::fst.metadata(path1)$NrOfRows)
  }
}

#' The first chunk of the disk.frame
#' firstchunk <- function(df) {
#'   eval(parse(text = sprintf("attr(%s,'chunk.seq') <- 1",as.character(substitute(df)) )),envir = parent.frame())
#'   attr(df, "chunk.seq") <- 1
#'   fst::read.fst(attr(df,"path"), from = 1, to = 1000000)
#' }
#' 
#' #' Gives the next chunk of the disk.frame
#' nextchunk <- function(df) {
#'   # df is a disk.frame chunk
#'   fromi <- (attr(df,"chunk.seq")+1)*1000000+1
#'   toi <- fromi + 1000000 -1
#'   
#'   dfstr <- as.character(substitute(df))
#'   eval(parse(text = sprintf("attr(%s,'chunk.seq') <- attr(%s,'chunk.seq') + 1 ", dfstr, dfstr)), envir = parent.frame())
#'   fst::read.fst(attr(df,"path"), from = fromi, to = toi)
#' }
#' 
#' start <- function(df) {
#'   
#' }

#' do to all chunks
#' @import fst
#' @import future
#' @import future.apply
#' @export
chunk_lapply <- function (...) {
  warning("chunk_lapply is deprecated in favour of map.disk.frame")
  map.disk.frame(...)
}

#' do to all chunks
#' @import fst
#' @import future
#' @import future.apply
#' @export
map <- function(...) UseMethod("map")

map.disk.frame <- function(df, fn, ..., outdir = NULL, keep=NULL, chunks = nchunk(df), compress = 50, lazy = T) {
  if(lazy) {
    attr(df, "lazyfn") = c(attr(df, "lazyfn"), fn)
    return(df)
  }
  
  if(!is.null(outdir)) {
    if(!dir.exists(outdir)) dir.create(outdir)
  }
  
  stopifnot(is_ready(df))
  keep1 = attr(df,"keep")
  
  if(is.null(keep)) {
    keep = keep1
  }
  
  path <- attr(df, "path")
  files <- dir(path, full.names = T)
  files_shortname <- dir(path)
  #browser()
  res = future_lapply(1:length(files), function(ii) {
    #res = fn(read_fst(files[ii], as.data.table=T, columns=keep), ...)
    
    res = fn(get_chunk.disk.frame(df, ii, keep=keep), ...)
    if(!is.null(outdir)) {
      if(!dir.exists(outdir)) dir.create(outdir)
      write_fst(res, file.path(outdir, files_shortname[ii]), compress)
      return(NULL)
    } else {
      return(res)
    }
  })
  #browser()
  if(!is.null(outdir)) {
    if(!dir.exists(outdir)) dir.create(outdir)
    return(disk.frame(outdir))
  } else {
    return(res)
  }
}

#' Lazy chunk_lapply wrapper
#' 
lazy <- function(df,...) {
  UseMethod("lazy")
}

lazy.disk.frame <- function(df, fn, ...) {
  chunk_lapply(df, fn, lazy=T, ...)
}

delayed <- function(df,...) {
  UseMethod("delayed")
}

delayed.disk.frame <- function(df, fn, ...) {
  chunk_lapply(df, fn, lazy=T, ...)
}
 
#' [ interface for disk.frame using fst backend
#' @import data.table
#' @import future
#' @import fst
#' @export
`[.disk.frame` <- function(df, i, j,..., keep = NULL) {
  # stopifnot(is_ready(df))
  #browser()
  res <- NULL
  fpath <- attr(df,"path")
  if(!dir.exists(fpath) & file.exists(fpath)) {
    md <- fst.metadata(fpath)
    ii <- sort(unique(round(seq(0, md$nrOfRows, length.out = 1+parallel::detectCores()))))
    
    res <- future_lapply(2:length(ii), function(k,i,j,dotdot) {
      #a <- fst::read.fst(fpath, columns = keep, from = ii[k-1]+1, to = ii[k], as.data.table = T)
      #a <- fst::read.fst(fpath, from = ii[k-1]+1, to = ii[k], as.data.table = T)
      if(dotdot == "NULL") {
        code = sprintf("a[%s,%s]", i, j)
      } else if (j == "NULL") {
        code = sprintf("a[%s]", i)
      } else {
        code = sprintf("a[%s,%s,%s]", i, j, dotdot)
      }
      
      aa <- eval(parse(text=code))
      rm(a); gc()
      aa
    }, deparse(substitute(i)), deparse(substitute(j)), deparse(substitute(...)))
  } else {
    ff <- dir(attr(df,"path"))
    
    res <- future_lapply(ff, function(k,i,j,dotdot) {
      if(dotdot == "NULL") {
        code = sprintf("a[%s,%s]", i, j)
      } else if (j == "NULL") {
        code = sprintf("a[%s]", i)
      } else {
        code = sprintf("a[%s,%s,%s]", i, j, dotdot)
      }
      a = get_chunk.disk.frame(df, k)
      
      aa <- eval(parse(text=code))
      #browser()
      rm(a); gc()
      aa
    }, deparse(substitute(i)), deparse(substitute(j)), deparse(substitute(...)))
  }
  
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

#' Distribute (chunk-up/break-up) a fst file into chunks into a folder
#' @param df The disk.frame
#' @param outdirpath The directory/folder to output the chunks to 
#' @param compress Level of compression: 0-100 where 100 is the highest level of compress
#' @export
distribute <- function(...) {
  UseMethod("distribute")
}

distribute.disk.frame <- function(df, outdirpath, compress = 100) {
  fpath <- attr(df, "path")
  md <- fst.metadata(fpath)
  ii <- sort(unique(round(seq(0, md$nrOfRows, length.out = 1+parallel::detectCores()))))
  
  if(!dir.exists(outdirpath)) {
    dir.create(outdirpath)
  }
  
  future_lapply(2:length(ii), function(k) {
    write_fst(fst::read.fst(fpath, from = ii[k-1]+1, to = ii[k], as.data.table = T), file.path(outdirpath, sprintf("fst%d", k-1)), compress = compress)
    gc()
  })
}

#' Returns names
# names <- function(x) {
#   UseMethod("names")
# }
# 
# names.default <- function(x) {
#   base::names(x)
# }
# 
# names.disk.frame <- function(x) {
#   path = attr(x,"path")
#   res = fst::fst.metadata(dir(path,full.names = T)[1])
#   res$columnNames
# }

# The mutate method
# mutate <- function(...) UseMethod("mutate")
# 
# mutate.default <- function(...) dplyr::mutate
# 
# mutate.disk.frame <- function(df,...) {
#   a <- firstchunk(df)
#   a <- dplyr::mutate(a, ...)
#   
#   save(a, attr(a,"path"))
#   while(!is.null(a <- nextchunk(a))) {
#     a <- dplyr::mutate(a, ...)
#     save(a, attr(a,"path"))
#   }
# }
