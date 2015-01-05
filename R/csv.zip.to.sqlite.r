#' Convert a csv file to sqlite format
#' 
#' @param file The zip file to convert into disk.frame format
#' @param file.name The file name of the file within the zip file to use
#' @param outpath The path (in the form of a folder) to output to sqlite. A
#'   disk.frame is folder containing multiple R data files
#' @param outtable The output table name in sqlite
#' @param read_fun The function used to read the files. The default is read.csv
#' @param safely Reads the file in chunks instead of in one go
#' @param VERBOSE Prints additionally messages or not
#' @param ... Parameters passed to read_fun
#' @import RSQLite
#' 
zip.file.to.sqlite <- function(file, file.name, outpath, outtable, read_fun = read.csv, ..., safely = TRUE, VERBOSE = FALSE)  {
  if (safely) {
    mb = 1048576 # number of bytes in megabyte
    memlimit = memory.limit() * mb # the memory limit    
    n = 50        
    f = unz(paste0(file), file.name)
    con = open(f) # open a connection to the file
    
    data <-read.csv(f,nrows=n,header=header, sep=sep, quote=quote)
    var.names = names(data)    
    provlinesize = pryr::object_size(data)/n  
    # now that I have determined n close the file
    close(f) 
    
    # the theory is the more you read in the first round the less likely your data format to be wrong
    f <- unz(paste0(csv,".zip"), "AO_ACCOUNTLEVEL_1406_FIX.csv")
    con <- open(f) # open a connection to the file again
    
    browser()
    
    n  <- floor(memlimit * 0.5 / provlinesize) # only use 75% of memory
    #total lines read
    totreadn <- n 
    print(paste("reading in", n, sep = " "))    
    print(system.time(data <-read.csv(f,nrows=n,header=header, sep=sep, quote=quote)))
    # a more realistic estimate of line size
    provlinesize <- pryr::object_size(data)  / n 
    
    #setting up sqlite
    con_data <- dbConnect(SQLite(), dbname=outpath)
    dbGetQuery(con_data,"PRAGMA journal_mode =   OFF")
    dbGetQuery(con_data,paste0("drop table if exists  ",outtable))
    
    # if not reached the end of line
    while(nrow(data) == n) { 
      gc() # garbage collect
      print(paste("writing to db",n,sep=" "))
      print(system.time(dbWriteTable(con_data, data, name = outtable, append = TRUE)))
      n  <- floor(memlimit * 0.5 / provlinesize)
      print(paste("reading in", n, sep = " "))      
      rm(data)
      gc() # garbage collect
      print(system.time(data <-read.csv(f, nrows = n, header = FALSE, sep=sep, quote=quote)))
      names(data) <- var.names      
      provlinesize <- (pryr::object_size(data)  + totreadn*provlinesize) / (totreadn + n)
      totreadn <- totreadn + n
    } 
    close(f)
    if (nrow(data) != 0 ) {
      n <- nrow(data)
      print(paste0("but only", n, "read. writing to db now"))
      print(system.time(dbWriteTable(con_data, data, name=outtable,append=TRUE, row.names = FALSE)))
    }
  } else {
    dbWriteTable(conn= dbConnect(SQLite(), dbname=outpath), name=outtable, value=csv, row.names = FALSE, header = TRUE, overwrite = TRUE)
  }
}