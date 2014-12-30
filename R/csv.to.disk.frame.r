# depends on data.table
#require(data.table)
csv.to.disk.frame <- function(infile, outpath, ..., mem.prop = 0.1) {  
  if (length(dir(outpath)) == 0) {
    dir.create(outpath)
  }
  
  Sys.info()["sysname"]
  mb = 1048576 # number of bytes in megabyte
  
  if(Sys.info()["sysname"] == "Windows") {
    memlimit = memory.limit() * mb # the memory limit
  } else {
    # assume 4 gigs
    memlimit = mb * 1024 * 4 
  }
    
  
  # the infile size
  # do NOT use file.size this function does not work on digitial ocean
  infile.size <- file.info(infile)$size
    
  # set a connection to the file so the file can be read nrows at a time
  f <- file(infile)
  
  # this doesn't seem to work
  #f = unz(paste0(csv,".zip"), "AO_ACCOUNTLEVEL_1406_FIX.csv")
  
  if(isOpen(f) | isIncomplete(f)) {    
    close(f)
  }
  file.con = open(f, open = "r") # open a connection to the file
  
  # read 50 lines to try and have a first guess at the size of the data
  data <-read.csv(f,nrows=50, ...)
  var.names = names(data)    
  #provlinesize = pryr::object_size(data)/50  
  provlinesize = object.size(data)/50  
  close(f, blocking =TRUE) # now that I have determined n
  
  # the theory is the more you read in the first round the less likely your data format to be wrong
  #f = unz(paste0(csv,".zip"), "AO_ACCOUNTLEVEL_1406_FIX.csv")
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
  
  # the approximate total file size fpr the first chunk
  tfs <- file.info(tf)$size/1024 * n
    
  # a more realistic estimate of line size
  provlinesize = object.size(data)  / n 
  
  #setting up disk.frame folder

  
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