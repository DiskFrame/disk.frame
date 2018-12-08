#install.packages(c("fst","future","data.table"))
if(!require(devtools)) install.packages("devtools")
if(!require(disk.frame)) devtools::install_github("xiaodaigh/disk.frame")

library(dplyr)
library(disk.frame)
library(data.table)
library(fst)
#library(future)
#library(future.apply)
#library(data.table)
nworkers = parallel::detectCores(logical=F)
cat(nworkers," workers\n")
#plan(multiprocess, workers = nworkers, gc = T)
#options(future.globals.maxSize=Inf)

rows_per_chunk = 1e7
# generate synthetic data
tmpdir = "tmpfst"
fs::dir_delete(tmpdir)

# write out 2*nworkers chunks
pt = proc.time()
df = disk.frame(tmpdir)
purrr::walk(1:(nworkers*2), function(ii) {
  system.time(ab <- data.table(a = runif(rows_per_chunk), b = runif(rows_per_chunk)) ) #102 seconds
  add_chunk(df, ab, ii)
})
cat("Generating data took: ", timetaken(pt), "\n")


# read and output the disk.frame as it to assess "sequential" read-write performance
pt = proc.time()
df2 <- map.disk.frame(df, ~.x, outdir = "tmpfst2", lazy = F, overwrite = T)
cat("Read and write took: ", timetaken(pt), "\n")

# get first few rows
head(df)

# get last few rows
tail(df)

# number of rows
nrow(df)

# number of columns
ncol(df)


# dplyr verbs -------------------------------------------------------------
df = disk.frame(tmpdir)

df %>%
  summarise(suma = sum(a)) %>% # this does a count per chunk
  collect(parallel = T)

# need a 2nd stage to finalise summing
df %>%
  summarise(suma = sum(a)) %>% # this does a count per chunk
  collect(parallel = T) %>% 
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
  collect(parallel = T)
cat("group by took: ", timetaken(pt), "\n")


# keep only one var is faster
pt = proc.time()
res1 <- df %>% 
  srckeep("a") %>% #keeping only the column `a` from the input
  summarise(suma = sum(a), n = n()) %>% 
  collect(parallel = T)
cat("summarise keeping only one column ", timetaken(pt), "\n")

# same operation without keeping
pt = proc.time()
res1 <- df %>% 
  summarise(suma = sum(a), n = n()) %>% 
  collect(parallel = T)
cat("summarise without keeping", timetaken(pt), "\n")
