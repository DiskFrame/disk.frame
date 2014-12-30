library(dplyr)
library(data.table)

disk.frame <- function(path, preview.rows = 1, ...) {
  #browser()
  first.file <-  file.path(path,sort(dir(path))[1])
  
  # df <- data.table::fread(first.file, nrows = nrows, ...)
  # load the first chunk into the environment of this function
  newe <- new.env()
  load(first.file, newe)
  
  df <- newe$a[1:preview.rows,]
  # the loaded variable sould be called a 
  class(df) <- c("disk.frame",class(df))
  
  attr(df,"path") <- path
  attr(df,"env") <- newe
  df
}

disk.lib <- function(path) {
  dir(path)
  # look inside the paths looking for disk.frame data sets.
}

`$.disk.frame`<- function(x,y) {  
  #b <- data.table::fread(attr(x,"file"))
  #browser()
  env <- attr(x,"env")
  env$a[,get(y)]
}

`[.disk.frame` <- function(df, chunk_id = -1, ...) {
  # b <- data.table::fread(attr(df,"file"),na.strings=".")
  # chunk_id <- -1 means all the chunks
  env <- attr(df,"env")
  env$a[...]
}

`$<-` <- function(x, name, value) {
  if("disk.frame" %in% class(x) ) {
    `$<-.disk.frame`(x,name,value)
  } else {
    dplyr::`$<-`(x,name,value)
  }
}

`$<-.disk.frame` <- function(x, name, value) {
  #browser()
  
    path <- attr(x,"path")
    for(p in sort(dir(path))) {
      print(paste0(which(p == dir(path)),"/",length(dir(path))))
      load(file.path(path,p))            
            
      name <- as.character(match.call()[3])
      a <- `$<-`(a, name, value)
      save(a, file = file.path(path,p))
      rm(a)
    }
}

firstchunk <- function(df) {
  #df is expected to be a disk.frame  
  path <- attr(df, "path")
  
  # obtain the first file
  first.file <-  file.path(path,sort(dir(path))[1])
  
  # df <- data.table::fread(first.file, nrows = nrows, ...)
  # load the first chunk into the environment of this function
  newe <- new.env()
  load(first.file, newe)
  
  df <- newe$a
  # the loaded variable sould be called a 
  #class(df) <- c("disk.frame.",class(df))
  
  attr(df,"path") <- path
  attr(df,"env") <- newe
  # assign chunk sequence
  attr(df,"chunk.seq") <- 1 
  df  
} 

nextchunk <- function(df) {
  # df is a disk.frame chunk
  chunk.seq <- attr(df,"chunk.seq") + 1 
  
  path <- attr(df, "path")
  
  # if no more chunks then return
  if(chunk.seq > length(dir(path))) {
    return(NULL)
  }
  # obtain the first file
  file <-  file.path(path,sort(dir(path))[chunk.seq])
  
  # df <- data.table::fread(first.file, nrows = nrows, ...)
  # load the first chunk into the environment of this function
  newe <- new.env()
  load(file, newe)
  
  df <- newe$a
  # the loaded variable sould be called a 
  #class(df) <- c("disk.frame.",class(df))
  
  attr(df,"path") <- path
  attr(df,"env") <- newe
  # assign chunk sequence
  attr(df,"chunk.seq") <- chunk.seq 
  df  
}

mutate <- function(...) UseMethod("mutate")

mutate.default <- function(...) dplyr::mutate

mutate.disk.frame <- function(df,...) {
  
  a <- firstchunk(df)
  a <- dplyr::mutate(a, ...)
  
  #save(a, attr(a,"file"))
  while(!is.null(a <- nextchunk(a))) {
    #a <- nextchunk(a)
    browser()
    a <- dplyr::mutate(a, ...)
    #save(a, attr(a,"file"))
  }
}


