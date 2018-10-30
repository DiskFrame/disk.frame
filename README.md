# disk.frame
A simple data manipulation library utilising on-disk stored data structure (primarily fst) for batch processing of large files.

# Example usage
```r
#install.packages(c("fst","future","data.table"))

if(!require(devtools)) install.packages("devtools")
if(!require(disk.frame)) devtools::install_github("xiaodaigh/disk.frame")

library(disk.frame)
library(fst)
library(future)
library(future.apply)
library(data.table)
nworkers = parallel::detectCores(logical=F)
#nworkers = parallel::detectCores()
cat(nworkers," workers\n")
plan(multiprocess, workers = nworkers, gc = T)
options(future.globals.maxSize=Inf)

rows_per_chunk = 1e7
# generate synthetic data
tmpdir = file.path("tmpfst")
fs::dir_create(tmpdir)

# write out nworkers chunks
pt = proc.time()
system.time(future_lapply(1:(nworkers*2), function(ii) {
  system.time(cars1m <- data.table(a = runif(rows_per_chunk), b = runif(rows_per_chunk)) ) #102 seconds
  write.fst(cars1m, file.path(tmpdir, ii), 100)
  # do not let write.fst be the last as it return the full data
  gc()
  NULL
}))
cat("Generating data took: ", timetaken(pt), "\n")

df = disk.frame(tmpdir)
# read and output
pt = proc.time()
system.time(df2 <- map.disk.frame(df, ~.x, outdir = "tmpfst2", lazy = F))
cat("Read and write out took: ", timetaken(pt), "\n")

# get first few rows
head(df)

# get last few rows
tail(df)

# number of rows
nrow(df)

# number of columns
ncol(df)
```

## Example: dplyr verbs
```r
df = disk.frame(tmpdir)

df %>%
  summarise(suma = sum(a)) %>% # this does a count per chunk
  collect
  
# need a 2nd stage to finalise summing
df %>%
  summarise(suma = sum(a)) %>% # this does a count per chunk
  collect %>% 
  summarise(suma = sum(suma)) 

# filter
pt = proc.time()
system.time(df_filtered <- df %>% 
              filter(a < 0.1))
cat("filtering a < 0.1 took: ", timetaken(pt), "\n")
nrow(df_filtered)

# group by
pt = proc.time()
res1 <- df %>% 
  filter(b < 0.1) %>% 
  mutate(blt005 = b < 0.05) %>% 
  group_by(blt005, hard = T) %>% # hard group_by is slower but avoid a 2nd stage aggregation
  summarise(suma = sum(a), n = n()) %>% 
  collect
cat("group by took: ", timetaken(pt), "\n")

# keep only one var is faster
pt = proc.time()
res1 <- df %>% 
  keep("a") %>% #keeping only the column `a` from the input
  summarise(suma = sum(a), n = n()) %>% 
  collect
cat("summarise keeping only one column ", timetaken(pt), "\n")

# same operation without keeping
pt = proc.time()
res1 <- df %>% 
  summarise(suma = sum(a), n = n()) %>% 
  collect
cat("summarise without keeping", timetaken(pt), "\n")
```

## Example: data.table syntax
```r
# count by chunks
system.time(df_cnt_by_chunk <- df[,.N])
pt = proc.time()
system.time(sum(df[,.N])) # need a 2nd stage of summary
cat("sum(df[,.N]) took: ", timetaken(pt), "\n")

# filter
pt = proc.time()
system.time(df_filtered <- df[a < 0.1,])
cat("df[a < 0.1,] took: ", timetaken(pt), "\n")
nrow(df_filtered)

# group by
pt = proc.time()
system.time(res1 <- df[b < 0.1,.(sum_a = sum(a), .N), by = b < 0.05])
cat("df[b < 0.1,.(sum_a = sum(a), .N), by = b < 0.05] took: ", timetaken(pt), "\n")
# res1 has performed group by for each of the 4 chunks need to further summarise
system.time(res2 <- res1[, .(sum(sum_a), sum(N)),b][, .(mean_a = V1/V2), b])
res2 # abit painful to create mean, but currently only this low level interface; will do a dplyr on top later

# keep only one var is faster
pt = proc.time()
system.time(df[,.(sum(a)), keep = "a"][,sum(V1)]) # 1.17
cat("df[,.(sum(a)), keep = 'a'] took: ", timetaken(pt), "\n")

# same operation without keeping
pt = proc.time()
system.time(df[,.(sum(a))][,sum(V1)]) #2.95
cat("df[,.(sum(a))] took: ", timetaken(pt), "\n")
```

