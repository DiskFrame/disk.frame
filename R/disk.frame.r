
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

# 
disk.frame_folder <- function(path, ....) {
  
}

#' Create a disk.frame from fst files
#' @path The path to store the output file or to a directory
disk.frame_fst <= function(path, ...) {
  df <- list()
  attr(df, "metadata") <- fst::fst.metadata(path)
  attr(df,"path") <- path
  attr(df,"backend") <- backend
  class(df) <- "disk.frame"
  df
}


#' Head
#' @export
head.disk.frame <- function(df, n = 6L, ...) {
  head(fst::read.fst(attr(df,"path"), from = 1, to = n), n = n, ...)
}

tail.disk.frame <- function(df, n = 6L, ...) {
  tail(fst::read.fst(attr(df,"path"), from = (df$NrOfRows-n+1), to = df$NrOfRows), n = n, ...)
}

#' Access an element of the disk.frame 
#' `$.disk.frame`<- function(x,y) {
#'   xx <- x
#'   class(xx) <- "list"
#'   fst::read.fst(.Primitive("$")(xx, "path"), columns = substitute(y))
#' }
#' 
#' #' The [ method of disk.frame
#' `[.disk.frame` <- function(df, chunk_id = -1, ...) {
#'   # b <- data.table::fread(attr(df,"file"),na.strings=".")
#'   # chunk_id <- -1 means all the chunks
#'   env <- attr(df,"env")
#'   env$data[...]
#' }
#' 
#' #' The $<- method of disk.frame
#' `$<-` <- function(x, name, value) {
#'   if("disk.frame" %in% class(x) ) {
#'     `$<-.disk.frame`(x,name,value)
#'   } else {
#'     dplyr::`$<-`(x,name,value)
#'   }
#' }
#' 
#' The $<- method of disk.frame
# `$<-.disk.frame` <- function(x, name, value) {
#     # path <- attr(x,"path")
#     # for(p in sort(dir(path))) {
#     #   print(paste0(which(p == dir(path)),"/",length(dir(path))))
#     #   load(file.path(path,p))
#     # 
#     #   name <- as.character(match.call()[3])
#     #   a <- `$<-`(a, name, value)
#     #   save(a, file = file.path(path,p))
#     #   rm(a)
#     # }
# }

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
chunks_lapply <- function(df, fn, ..., outdir=NULL, chunks = 16, parallel = F) {
  if(parallel) {
    ii <- seq(0,df$NrOfRows, length.out = chunks)
    future_lapply(2:length(ii), function(i) {
      fn(read.fst(attr(df,"path"), from = ii[i-1]+1, to = ii[i], as.data.table=T))
    })
  } else {
    browser()
    chunks_xapply(df, fn, lapply, ..., outdir = outdir, chunks = chunks)
  }
}

chunks_sapply <- function(df, fn, ..., outdir=NULL, chunks = 16) {
  chunks_xapply(df, fn, chunks, sapply, ..., outdir=outdir)
}

chunks_xapply <- function(df, fn, xapply, ..., outdir=NULL, chunks = 16) {
  ii <- seq(0,attr(df,"metadata")$NrOfRows, length.out = chunks)
  browser()
  xapply(2:length(ii), function(i, ...) {
    browser()
    res <- fn(fst::read.fst(attr(df,"path"), from = ii[i-1]+1, to = ii[i], as.data.table=T), ...)
    
    if(!is.null(outdir)) {
      fst::write.fst(res, file.path(outdir,paste0(i,".chunk")))
    }
    res
  }, ...)
}

#' Yeah yeah yeah
#' @import data.table
`[.disk.frame` <- function(df, i,j,...) {
  ii <- seq(0,attr(df,"metadata")$NrOfRows, length.out = 16)
  res <- lapply(2:length(ii), function(k,i,j,dotdot) {
    a <- fst::read.fst(attr(df,"path"), from = ii[k-1]+1, to = ii[k], as.data.table = T)
    if(deparse(dotdot) == "NULL") {
      code = sprintf("a[%s,%s]", deparse(i), deparse(j))
    } else {
      code = sprintf("a[%s,%s,%s]", deparse(i), deparse(j), deparse(dotdot))
    }
    
    eval(parse(text=code))
  }, substitute(i),substitute(j), substitute(...))
  
  rbindlist(res)
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
