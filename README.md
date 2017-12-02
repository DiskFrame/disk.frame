# disk.frame
A simple data manipulation library utilising on disk stored data structure (primarily fst) for batch processing of large files.

# Example usage

```r
if(!require(devtools)) install.packages("devtools")
devtools::install_github("xiaodaigh/disk.frame")
library(disk.frame)

library(fst)
library(future)
library(data.table)
#nworkers = parallel::detectCores(logical=F)
nworkers = parallel::detectCores()
plan(multiprocess, workers = nworkers, gc = T)
options(future.globals.maxSize=Inf)

row_per_chunk = 1e7
# generate synthetic data
tmpdir = file.path("tmpfst")
dir.create(tmpdir)

# write out 4 chunks
system.time(future_lapply(1:8, function(ii) {
  system.time(cars1m <- data.table(a = runif(row_per_chunk), b = runif(row_per_chunk)) ) #102 seconds
  write.fst(cars1m, file.path(tmpdir, ii), 100)
  # do not let write.fst be the last as it return the full data
  gc()
  NULL
}))

# read and output
system.time(df2 <- chunk_lapply(df, function(df) df, outdir = "tmpfst2"))

# get first few rows
head(df)

# nrows
nrow(df)

# count by chunks
system.time(df[,.N])
system.time(sum(df[,.N])) # need a 2nd stage of summary

# filter
system.time(df_filtered <- df[a < 0.1,])
base::nrow(df_filtered) # beacuse of bug in disk.frame

# group by
system.time(res1 <- df[b < 0.1,.(sum_a = sum(a), .N), by = b < 0.05])
# res1 has performed group by for each of teh 4 chunks need to further summarise
system.time(res2 <- res1[, .(sum(sum_a), sum(N)),b][, .(mean_a = V1/V2), b])
res2 # abit painful to create mean, but currently only this low level interface; will do a dplyr on top later

# keep only one var is faster
system.time(df[,.(sum(a)), keep = "a"][,sum(V1)]) # 1.17

# same operation without keeping
system.time(df[,.(sum(a))][,sum(V1)]) #2.95
```

