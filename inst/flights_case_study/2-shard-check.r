library(disk.frame)
plan(multiprocess)
library(magrittr)

a = list.files("inst/flights_case_study/data", 
           pattern="*.csv", 
           full.names = T) %>% 
  head(10) %>%  # only 10 files so that we can test faster
  purrr::map(~ csv_to_disk.frame(.x, 
                                 outdir=paste0("tmp", readr::parse_number(.x)),
                                 shardby = c("Year","Month"),
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(outdir = "flights_all_sharded.df", by_chunk_id = T)


b = list.files("inst/flights_case_study/data", 
           pattern="*.csv", 
           full.names = T) %>% 
  head(10) %>%  # only 10 files so that we can test faster
  purrr::map(~ csv_to_disk.frame(.x, 
                                 outdir=paste0("tmp", readr::parse_number(.x)),
                                 nchunks = 128,
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(outdir = "flights_all_notsharded.df", by_chunk_id = T)

