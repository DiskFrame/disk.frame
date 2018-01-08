function() {
  csvpath = "D:/git/quick-sas7bdat/tests/beer_chunks"
  
  # csv = dir(csvpath,full.names = T)
  # readytoload = csv[substr(csv,nchar(csv)-2,nchar(csv)) == "csv"]
  library(data.table)
  library(future)
  library(fst)
  plan(multiprocess)
  library(stringr)
  
  nfiles = 100
  taken = rep(F, nfiles)
  taken = taken | file.exists(file.path(csvpath,sprintf("out_%d.fst",0:(nfiles-1))))
  
  while(!all(taken)) {
    w = which(!taken)
    if(length(w) == 0) {
      stop("done")
    } else {
      for(ww in w) {
        csvfile = file.path(csvpath,sprintf("out_%d.csv",ww-1))
        if(file.exists(csvfile)) {
          taken[ww] = T
          print(ww)
          
          waitfuture %<-% {
            res = data.table::fread(csvfile)
            fst::write.fst(res, str_replace(csvfile,".csv",".fst"), 100)
            unlink(csvfile)
            gc()
          }
        }
      }
    }
  }# took 15 mins for sgb mortgages
  
  df = disk.frame("fm.df")
  nrow(df)/1000/1000/1000
  
  system.time(df1 <- rbindlist(future_lapply(dir("fm.df", full.names = T), function(dpath) {
    read_fst(dpath, as.data.table = T)
    gc()
  })))
  
  head(df)
  
  df[,n_distinct(V1)]
}
