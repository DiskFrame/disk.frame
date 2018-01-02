library(data.table)
library(fst)
library(future)
library(stringr)
#nworkers = 2 # took 2 hours and 20 minutes
nworkers = 4 # number of background session
plan(multiprocess, workers = nworkers)

# sas file to convert
sasfile = '"c:/path/to/input_sas.sas7bdat"'

# path to input csv folder
path = "C:/path/to/outputfst_folder"

# path to executable
sas2csvpath = "C:/path/to/sas2csv.exe"

# output fst folder
out_fst_path = "df.df"

# number of chunks to output
nchunks = 100

# output fst files
files = file.path(out_fst_path, paste0(0:(nchunks-1),".fst"))

# checks if the files are ready
ready = rep(F, nchunks) | file.exists(files)

# checks if the extracting are ready
extracting = rep(F, nchunks)

# function to convert some chunks at a time
sas2csv <- function(chunks) {  
  dir.create(file.path("outcsv",as.character(chunks)))
  options = sprintf("-o outcsv/%s/ -c -n %d -k %s -m", chunks, nchunks, paste(chunks,collapse = " "))  
  cmd = paste(sas2csvpath, sasfile, options)
  system(cmd)
}

dir.create(out_fst_path)

while(!all(ready)) {
  done1 = F
  extracting_jobs = F
  for(w in which(!ready)) {
    incsv = file.path("outcsv", w-1,paste0("_", w-1,".csv"))
    if(file.exists(incsv)) {
      done1 = T
      ready[w] = T
      ok %<-% {
        write_fst(fread(incsv), file.path(out_fst_path, paste0(w-1,".fst")), 100)
        #unlink(incsv)
        file.remove(incsv)
        gc()
      }
      print(paste("converting: ", w-1, Sys.time()))
    } else if (!extracting_jobs & !extracting[w]) {
      done1 = T
      extracting_jobs = T
      extracting[w] <- T
      ok %<-% {
        sas2csv(w-1)
      }
      print(paste("extracting: ",w-1, Sys.time()))
    }
  }
  if(!done1) {
    print(paste0("didn't get any work :(", Sys.time()))
  }
}