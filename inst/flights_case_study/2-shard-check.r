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


aa = a[,.N, UniqueCarrier, keep=c("UniqueCarrier")][,sum(N), UniqueCarrier][order(UniqueCarrier)]

bb = b[,.N, UniqueCarrier, keep=c("UniqueCarrier")][,sum(N), UniqueCarrier][order(UniqueCarrier)]

identical(aa,bb)

a2 = a %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=n()) %>% 
  collect %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=sum(n)) %>% 
  order_by(UniqueCarrier)

b2 = b %>% group_by(UniqueCarrier) %>% 
  summarise(n=count()) %>% 
  collect %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=sum(n))

