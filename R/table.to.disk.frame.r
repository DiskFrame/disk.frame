#' Convert a table file to disk.frame format
#' 
#' @param infile The csv file to convert into disk.frame format
#' @param outpath The path (in the form of a folder) to output the disk.frame. A
#'   disk.frame is folder containing multiple R data files
#' @param mem.prop The proportion of memory to use. On Windows the memory limit 
#'   is determined using the memory.limit() function while on other platforms 
#'   the avaialble memory is assumed to be 4Gb. So by default on Windows it will
#'   use 10\% of the memory while on Linux it will use 400mb
#' @param read_fun The function used to read in the table. The funciton needs t
#'   have parameters nrows and header (as a boolean) arguments defined
#' @param ... Parameters passed to read.fun and table.to.disk.frame
#' @param VERBOSE Prints additional error messages
#' @importFrom pryr object_size
#' @export
#'   
table.to.disk.frame <- function(infile, outpath, ..., mem.prop = 0.1, VERBOSE = FALSE, read_fun = read.table ) {  
  if (length(dir(outpath)) == 0) {
    dir.create(outpath)
  }
  
  mb = 1048576 # number of bytes in megabyte
  
  if(Sys.info()["sysname"] == "Windows") {
    # the memory limit
    memlimit = memory.limit() * mb 
  } else {
    # assume you have at least 4 gigs of RAM to play with
    memlimit = mb * 1024 * 4 
  }
  
  # the infile size
  # do NOT use file.size this function does not work on Digital Ocean
  infile.size <- file.info(infile)$size
  
  # set a connection to the file so the file can be read nrows at a time
  f <- file(infile)
  
  # close the connection if the file is already open
  if(isOpen(f) | isIncomplete(f)) {    
    close(f)
  }
  
  # open a connection to the file
  file.con = open(f, open = "r") 
  
  # read 50 lines to try and have a first guess at the size of the data
  data <-read_fun(f, nrows=50, ...)
  var.names = names(data)    
  
  provlinesize = object_size(data)/50  
  # now that I have determined n close the file
  close(f, blocking =TRUE) 
  
  # the theory is the more you read in the first round the less likely your data format to be wrong  
  f <- file(infile)
  # open a connection to the file again
  file.con = open(f, open ="r") 
  
  # only use mem.prop of memory
  n  = floor(memlimit * mem.prop / provlinesize)
  
  #total lines read
  totreadn = n
  chunk = 1
  
  # n.test.rows is the number of rows to write to disk to aid with determining the size of the file
  if(VERBOSE) 
    print(paste0("reading in ", n, " rows as chunk number: 1"))
  
  # read in a csv
  system.time(data <- data.table(read_fun(f, nrows=n, ...)))
  
  if (VERBOSE) print(.Last.Value)
  
  # write some rows to disk to get an idea of file size
  tf <- tempfile()
  write.csv(data[1:1000,], file = tf, row.names = FALSE, col.names = FALSE)
  
  # the approximate total file size in bytes for the first chunk
  tfs <- file.info(tf)$size/1000 * n
  
  # a more realistic estimate of line size
  provlinesize = object_size(data)  / n 
  
  
  # if not reached the end of line
  # if the number of rows read in the same as n then we have not reached the end of the line
  while(nrow(data) == n) {     
    # this line needs to be here    
    if(VERBOSE) print(paste0("writing to disk.frame ",n, " rows"))
    # print(system.time(save(data, file = file.path(outpath,paste0(chunk,".disk.frame")))))
    cat(paste0("~",min(round(tfs / infile.size * 100,0),100),"% complete"))
    
    n  = floor(memlimit * mem.prop / provlinesize)
    
    chunk <- chunk + 1
    if (VERBOSE) print(paste0("reading in ", n," rows as chunk number: ", chunk))
    
    system.time(data <- data.table(read_fun(f, nrows = n, header = FALSE, ...)))
    
    if (VERBOSE) print(.Last.value)
    
    names(data) <- var.names      
    
    # write a file to disk
    tf <- tempfile()
    write.csv(data[1:1000,], file = tf, row.names = FALSE, col.names = FALSE)
    
    # the total file size in bytes including header
    tfs <- file.info(tf)$size/1000*n + tfs
    
    provlinesize <- (object.size(data)  + totreadn*provlinesize) / (totreadn + n)
    totreadn <- totreadn + n        
  }
  
  # reached the end of the file once out of the above while loop
  # so close the connection
  close(f)
  
  if (nrow(data) != 0 ) {
    n = nrow(data)
    if(VERBOSE) print(paste0("but only ", n, " read. writing chunk number: ", chunk))
    # save the data into a new chunk
    system.time(save(data, file = file.path(outpath,paste0(chunk,".disk.frame"))))
    if(VERBOSE) print(.Last.value)
  }
}

#' @rdname table.to.disk.frame
#' @export
csv.to.disk.frame <- function(...) {  
  table.to.disk.frame(..., read_fun = read.csv)  
}