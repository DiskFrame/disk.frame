library(fst)
library(future)
library(data.table)
#nworkers = parallel::detectCores(logical=F)
nworkers = parallel::detectCores()
plan(multiprocess, workers = nworkers, gc = T)
options(future.globals.maxSize=Inf)

# generate synthetic data
#tmpdir = file.path(tempdir(),"tmpfst")
tmpdir = file.path("tmpfst")
dir.create(tmpdir)

# write out 4 chunks
system.time(future_lapply(1:8, function(ii) {
  system.time(cars1m <- data.table(a = runif(1e9/8), b = runif(1e9/8)) ) #102 seconds
  write.fst(cars1m, file.path(tmpdir, ii), 100)
  # do not let write.fst be the last as it return the full data
  gc()
  NULL
}))