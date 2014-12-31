#' Convert a csv file to disk.frame format
#' 
#' @param infile The csv file to convert into disk.frame format
#' @param outpath The path (in the form of a folder) to output the disk.frame. A
#'   disk.frame is folder containing multiple R data files
#' @param mem.prop The proportion of memory to use. On Windows the memory limit 
#'   is determined using the memory.limit() function while on other platforms 
#'   the avaialble memory is assumed to be 4Gb. So by default on Windows it will
#'   use 10% of the memory while on Linux it will use 400mb
#' @param ... Parameters passed to read.table
#' 
#'
csv.to.disk.frame <- function(infile, outpath, ..., mem.prop = 0.1) {  
  if (length(dir(outpath)) == 0) {
    dir.create(outpath)
  }
  
  mb = 1048576 # number of bytes in megabyte
  
  if(Sys.info()["sysname"] == "Windows") {
    memlimit = memory.limit() * mb # the memory limit
  } else {
    # assume you have at least 4 gigs of RAM to play with
    memlimit = mb * 1024 * 4 
  }
  
  # the infile size
  # do NOT use file.size this function does not work on Digitial Ocean
  infile.size <- file.info(infile)$size
    
  # set a connection to the file so the file can be read nrows at a time
  f <- file(infile)
  
  if(isOpen(f) | isIncomplete(f)) {    
    close(f)
  }
  file.con = open(f, open = "r") # open a connection to the file
  
  # read 50 lines to try and have a first guess at the size of the data
  data <-read.csv(f,nrows=50, ...)
  var.names = names(data)    
  
  # can't seem to install pryr on Digital Ocean (could be a case of small instance)
  #provlinesize = pryr::object_size(data)/50  
  provlinesize = object.size(data)/50  
  close(f, blocking =TRUE) # now that I have determined n
  
  # the theory is the more you read in the first round the less likely your data format to be wrong
  
  f <- file(infile)
  file.con = open(f, open ="r") # open a connection to the file again
  
  # only use mem.prop of memory
  n  = floor(memlimit * mem.prop / provlinesize)
  
  #total lines read
  totreadn = n
  chunk = 1
  
  # n.test.rows is the number of rows to write to disk to aid. Aims to be 1mb
  
  print(paste0("reading in ", n, " rows as chunk number: 1"))    
  print(system.time(data <-data.table(read.csv(f, nrows=n, ...))))
  
  # write some rows to disk to get an idea of file size
  tf <- tempfile()
  write.csv(data[1:1024,], file = tf, row.names = FALSE, col.names = FALSE)
  
  # the approximate total file size in bytes for the first chunk
  tfs <- file.info(tf)$size/1024 * n
    
  # a more realistic estimate of line size
  provlinesize = object.size(data)  / n 
  
  
  while(nrow(data) == n) { # if not reached the end of line    
    # this line needs to be here
    
    print(paste0("writing to disk.frame ",n, " rows"))
    # print(system.time(save(data, file = file.path(outpath,paste0(chunk,".disk.frame")))))
    cat(paste0("~",min(round(tfs / infile.size * 100,0),100),"% complete"))
        
    n  = floor(memlimit * mem.prop / provlinesize)
    
    chunk <- chunk + 1
    print(paste0("reading in ", n," rows as chunk number: ", chunk))
    #rm(data)
    print(system.time(data <-data.table(read.csv(f, nrows = n, header = FALSE, ...))))
    names(data) <- var.names      
    
    # write a file to disk
    tf <- tempfile()
    write.csv(data[1:1024,], file = tf, row.names = FALSE, col.names = FALSE)
    
    # the total file size in bytes including header
    tfs <- file.info(tf)$size/1024*n + tfs
    
    
    provlinesize <- (object.size(data)  + totreadn*provlinesize) / (totreadn + n) #95% CI of line size
    totreadn <- totreadn + n        
  }
  close(f)
  if (nrow(data) != 0 ) {
    n = nrow(data)
    print(paste0("but only ", n, " read. writing chunk number: ", chunk))
    print(system.time(save(data, file = file.path(outpath,paste0(chunk,".disk.frame")))))
  }
}