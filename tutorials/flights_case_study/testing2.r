#install.packages("tidyverse")
library(readr)
library(disk.frame)
library(data.table)
library(future)
library(magrittr)
plan(multiprocess)

pt=proc.time()
a = list.files("inst/flights_case_study/data/", 
           pattern="*.csv", 
           full.names = T) %>% 
  head(10) %>%  # only 10 files so that we can test faster
  purrr::map(~ csv_to_disk.frame(.x, 
                                 outdir=paste0("tmp", readr::parse_number(.x)),
                                 nchunks = 128,
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(outdir = "flights_all_notsharded.df", by_chunk_id = T)
print(timetaken(pt))

pt=proc.time()
b = list.files("inst/flights_case_study/data/", 
           pattern="*.csv", 
           full.names = T) %>% 
  head(10) %>%  # only 10 files so that we can test faster
  purrr::map(~ csv_to_disk.frame(.x, 
                                 outdir=paste0("tmp", readr::parse_number(.x)),
                                 shardby = c("Year","Month"),
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(outdir = "flights_all_sharded.df", by_chunk_id = T)
print(timetaken(pt))