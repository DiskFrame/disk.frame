# library(dplyr)
# library(data.table)

#' Create a disk.frame
#' @param path The path to store the output file or to a directory
#' @param preview.rows The number of rows to load into memory
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
  class(df) <- "disk.frame"
  attr(df, "metadata") <- sapply(files,function(file1) fst::fst.metadata(file1))
  df
}



#' Create a disk.frame from fst files
#' @path The path to store the output file or to a directory
#' @import fst
#' @export
disk.frame_fst <- function(path, ...) {
  df <- list()
  attr(df, "metadata") <- fst::fst.metadata(path)
  attr(df,"path") <- path
  attr(df,"backend") <- "fst"
  class(df) <- "disk.frame"
  df
}

# 
# head <- function(...) {
#   UseMethod("head")
# }
# 
# head.default <- function(...) {
#   base::head(...)
# }
# 
# tail <- function(...) {
#   UseMethod("tail")
# }
# 
# tail <- function(...) {
#   base::tail(...)
# }


#' Head
#' @export
head.disk.frame <- function(df, n = 6L, ...) {
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)[1]
    head(fst::read.fst(path2, from = 1, to = n), n = n, ...)
  } else {
    head(fst::read.fst(path1, from = 1, to = n), n = n, ...)
  }
}

tail.disk.frame <- function(df, n = 6L, ...) {
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)
    path2 <- path2[length(path2)]
    tail(fst::read.fst(path2, from = 1, to = n), n = n, ...)
  } else {
    tail(fst::read.fst(path1, from = (df$NrOfRows-n+1), to = df$NrOfRows), n = n, ...)
  }
}

nrow <- function(...) {
  UseMethod("nrow")
}

nrow.disk.frame <- function(df) {
  path1 <- attr(df,"path")
  if(dir.exists(path1)) {
    path2 <- dir(path1,full.names = T)
    return(sum(sapply(path2, function(p2) fst::fst.metadata(p2)$nrOfRows)))
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
# chunks_lapply <- function(df, fn, ..., outdir=NULL, chunks = 16, parallel = F) {
#   if(parallel) {
#     ii <- seq(0,df$NrOfRows, length.out = chunks)
#     future_lapply(2:length(ii), function(i) {
#       fn(read.fst(attr(df,"path"), from = ii[i-1]+1, to = ii[i], as.data.table=T))
#     })
#   } else {
#     chunks_xapply(df, fn, lapply, ..., outdir = outdir, chunks = chunks)
#   }
# }
# 
# chunks_sapply <- function(df, fn, ..., outdir=NULL, chunks = 16) {
#   chunks_xapply(df, fn, chunks, sapply, ..., outdir=outdir)
# }
# 
# chunks_xapply <- function(df, fn, xapply, ..., outdir=NULL, chunks = 16) {
#   ii <- seq(0,attr(df,"metadata")$NrOfRows, length.out = chunks)
#   browser()
#   xapply(2:length(ii), function(i, ...) {
#     browser()
#     res <- fn(fst::read.fst(attr(df,"path"), from = ii[i-1]+1, to = ii[i], as.data.table=T), ...)
#     
#     if(!is.null(outdir)) {
#       fst::write.fst(res, file.path(outdir,paste0(i,".chunk")))
#     }
#     res
#   }, ...)
# }


# `[.disk.frame` <- function(df, i,j,...) {
#   ii <- seq(0,attr(df,"metadata")$NrOfRows, length.out = 16)
#   res <- lapply(2:length(ii), function(k,i,j,dotdot) {
#     a <- fst::read.fst(attr(df,"path"), from = ii[k-1]+1, to = ii[k], as.data.table = T)
#     if(deparse(dotdot) == "NULL") {
#       code = sprintf("a[%s,%s]", deparse(i), deparse(j))
#     } else {
#       code = sprintf("a[%s,%s,%s]", deparse(i), deparse(j), deparse(dotdot))
#     }
#     
#     eval(parse(text=code))
#   }, substitute(i),substitute(j), substitute(...))
#   
#   rbindlist(res)
# }

#' Yeah yeah yeah
#' @import data.table
#' @import future
#' @import fst
`[.disk.frame` <- function(df, i,j,..., keep = NULL) {
  res <- NULL
  fpath <- attr(df,"path")
  if(!dir.exists(fpath) & file.exists(fpath)) {
    md <- fst.metadata(fpath)
    ii <- sort(unique(round(seq(0, md$nrOfRows, length.out = 1+parallel::detectCores()))))
    
    res <- future_lapply(2:length(ii), function(k,i,j,dotdot) {
      a <- fst::read.fst(fpath, columns = keep, from = ii[k-1]+1, to = ii[k], as.data.table = T)
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
    ff <- dir(attr(df,"path"), full.names = T)
    
    res <- future_lapply(ff, function(k,i,j,dotdot) {
      if(dotdot == "NULL") {
        code = sprintf("a[%s,%s]", i, j)
      } else if (j == "NULL") {
        code = sprintf("a[%s]", i)
      } else {
        code = sprintf("a[%s,%s,%s]", i, j, dotdot)
      }
      a <- fst::read.fst(k, columns = keep, as.data.table = T)
      aa <- eval(parse(text=code))
      #browser()
      rm(a); gc()
      aa
    }, deparse(substitute(i)), deparse(substitute(j)), deparse(substitute(...)))
  }
  
  # sometimes the returned thing is a vetor e.g. df[,.N]
  if("data.frame" %in% class(res[[1]])) {
    return(rbindlist(res))
  } else  if(is.vector(res)) {
    return(unlist(res))
  } else {
    warning("spooky")
    return(res)
  }
    
}

distribute <- function(...) {
  UseMethod("distribute")
}

distribute.disk.frame <- function (df, outdirpath, compress = 0) {
  fpath <- attr(df, "path")
  md <- fst.metadata(fpath)
  ii <- sort(unique(round(seq(0, md$nrOfRows, length.out = 1+parallel::detectCores()))))
  
  if(!dir.exists(outdirpath)) {
    dir.create(outdirpath)
  }
  
  future_lapply(2:length(ii), function(k) {
    write.fst(fst::read.fst(fpath, from = ii[k-1]+1, to = ii[k], as.data.table = T), file.path(outdirpath, sprintf("fst%d", k-1)), compress = compress)
    gc()
  })
}

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
