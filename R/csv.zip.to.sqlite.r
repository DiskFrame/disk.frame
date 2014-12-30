csv.zip.to.sqlite <- function(csv,outpath,outtable,safely=TRUE, header = TRUE, sep=",", quote='"', verbose = FALSE)  {
  require(RSQLite)  
  if (safely) {
    mb = 1048576 # number of bytes in megabyte
    memlimit = memory.limit() * mb # the memory limit
    
    n = 50    
    f = unz(paste0(csv,".zip"), "AO_ACCOUNTLEVEL_1406_FIX.csv")
    con = open(f) # open a connection to the file
    
    data <-read.csv(f,nrows=n,header=header, sep=sep, quote=quote)
    var.names = names(data)    
    provlinesize = pryr::object_size(data)/n  
    close(f) # now that I have determined n
    
    # the theory is the more you read in the first round the less likely your data format to be wrong
    f = unz(paste0(csv,".zip"), "AO_ACCOUNTLEVEL_1406_FIX.csv")
    con = open(f) # open a connection to the file again
    browser()
    n  = floor(memlimit * 0.5 / provlinesize) # only use 75% of memory
    totreadn = n #total lines read
    print(paste("reading in", n, sep=" "))    
    print(system.time(data <-read.csv(f,nrows=n,header=header, sep=sep, quote=quote)))
    provlinesize = pryr::object_size(data)  / n # a more realistic estimate of line size
    
    #setting up sqlite
    con_data = dbConnect(SQLite(), dbname=outpath)
    dbGetQuery(con_data,"PRAGMA journal_mode =   OFF")
    dbGetQuery(con_data,paste0("drop table if exists  ",outtable))
    
    while(nrow(data) == n) { # if not reached the end of line
      gc() # garbage collect
      print(paste("writing to db",n,sep=" "))
      print(system.time(dbWriteTable(con_data, data, name = outtable, append = TRUE)))
      n  = floor(memlimit * 0.5 / provlinesize)
      print(paste("reading in", n, sep = " "))      
      rm(data)
      gc() # garbage collect
      print(system.time(data <-read.csv(f, nrows = n, header = FALSE, sep=sep, quote=quote)))
      names(data) <- var.names      
      provlinesize = (pryr::object_size(data)  + totreadn*provlinesize) / (totreadn + n) #95% CI of line size
      totreadn = totreadn + n
    } 
    close(f)
    if (nrow(data) != 0 ) {
      n = nrow(data)
      print(paste0("but only", n, "read. writing to db now"))
      print(system.time(dbWriteTable(con_data, data, name=outtable,append=TRUE, row.names = FALSE)))
    }
  } else {
    dbWriteTable(conn= dbConnect(SQLite(), dbname=outpath), name=outtable, value=csv, row.names = FALSE, header = TRUE, overwrite = TRUE)
  }
}