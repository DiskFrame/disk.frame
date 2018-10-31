# Introduction

The `disk.frame` package aims to be the answer to one question: how do I manipulate structured tabular data that doesn't fit into Random Access Memory (RAM)? `disk.frame` has a two-part answer to this question:

1) split up a large-than-RAM dataset into chunks and store each chunk in a separate file inside a folder and 
2) provide a convenient API to manipulate these chunks

That is `disk.frame` in a nutshell.

`disk.frame` performs a similar role to distributed systems such as Apache Spark, Python's Dask, and Julia's JuliaDB.jl for *medium data* which are datasets that are too large for RAM but not quite large enough to qualify as *big data* that require distributing processing over many computers to be effective.

## Common questions {#basics-1}

### a) What is `disk.frame` and why create it? {#what-is-`disk.frame`-1a}

`disk.frame` is an R package that provides a framework for manipulating larger-than-RAM structured tabular data on disk efficiently. The reason one would want to manipulate data on disk is that it allows arbitrarily large datasets to be processed by R; hence relaxing the assumption from "R can only deal with data that fits in RAM" to be being able to "deal with data that fits on disk". See the next section.

### b) How is it different to `data.frame`s and `data.table`s?

A `data.frame` in R is an in-memory data structure, which means that R must load the data contained within in its entirety into RAM. A corollary of this is that only data that can fit into RAM can be processed using `data.frame`s. This places significant restrictions on what R can process with minimal hassle.

In contrast, `disk.frame` provides a framework to store and manipulate data on the hard drive. It does this by loading only a small part of the data, called a chunk, into RAM; process the chunk, write out the results and repeat with the next chunk. This chunking strategy is widely applied in other packages to enable processing large amounts of data in R, for example, see [`chunkded`](https://github.com/edwindj/chunked) [`arkdb`](https://github.com/ropensci/arkdb), and [`iotools`](https://github.com/s-u/iotools).

Furthermore, there is a row-limit of 2^31 for `data.frame`s in R; hence an alternate approach is needed to apply R to these large datasets. The chunking mechanism in `disk.frame` provides such an avenue to enable data manipulation beyond the 2^31 row limit.

### c) How is `disk.frame` different to previous "big" data solutions for R?

R has many packages that can deal with larger-than-RAM datasets, including `ff` and `bigmemory`. However, `ff` and `bigmemory` restrict the user to primitive data types such as double, which means they do not support character (string) and factor types. In contrast, `disk.frame` makes use of `data.table::data.table` and `data.frame` directly, so all data types are supported. Also, `disk.frame` strives to provide an API that is as similar to `data.frame`'s where possible. `disk.frame` supports many `dplyr` verbs for manipulating `disk.frame`s.

Additionally, `disk.frame` supports parallel data operations using infrastructures provided by the excellent [`future` package](https://cran.r-project.org/web/packages/future/vignettes/future-1-overview.html) to take advantage of multi-core CPUs. Further, `disk.frame` uses state-of-the-art data storage techniques such as fast data compression, and random access to rows and columns provided by the [`fst` package](http://www.fstpackage.org/) to provide superior data manipulation speeds.

### d) How does `disk.frame` work?

`disk.frame` works by breaking large datasets into smaller individual chunks and storing the chunks in `fst` files inside a folder. Each chunk is a `fst` file containing a `data.frame/data.table`. One can construct the original large dataset by loading all the chunks into RAM and row-bind all the chunks into one large `data.frame`. Of course, in practice this isn't always possible; hence why we store them as smaller individual chunks.

`disk.frame` makes it easy to manipulate the underlying chunks by implementing `dplyr` functions/verbs and other convenient functions (e.g. the `map.disk.frame(a.disk.frame, fn, lazy = F)` function which applies the function `fn` to each chunk of `a.disk.frame` in parallel). So that `disk.frame` can be manipulated in a similar fashion to in-memory `data.frame`s.

### e) How is `disk.frame` different from Spark, Dask, and JuliaDB.jl?

Spark is primarily a distributed system that also works on a single machine. Dask is a Python package that is most similar to `disk.frame`, and JuliaDB.jl is a Julia package. All three can distribute work over a cluster of computers. However, `disk.frame` currently cannot distribute data processes over many computers, and is, therefore, single machine focused.

In R, one can access Spark via `sparklyr`, but that requires a Spark cluster to be set up. On the other hand `disk.frame` requires zero-setup apart from running `install.packages("disk.frame")` or `devtools::install_github("xiaodaigh/disk.frame")`. 

Finally, Spark can only apply functions that are implemented for Spark, whereas `disk.frame` can use any function in R including user-defined functions.

# Example usage
```r
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

rows_per_chunk = 1e8
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
