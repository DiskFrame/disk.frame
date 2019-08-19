  library(disk.frame)
  
  pt = proc.time()
  non_shard = 2007:2008 %>% 
    purrr::map(~ csv_to_disk.frame(paste0("inst/flights_case_study/data/",.x,".csv"), 
                                   outdir=paste0("tmp",.x),
                                   overwrite=T)) %>% 
    rbindlist.disk.frame(
      outdir = "inst/data/flights_all_no_shard.df", by_chunk_id = T)
  
  
  sharded = 2007:2008 %>% 
    purrr::map(~ csv_to_disk.frame(paste0("inst/flights_case_study/data/",.x,".csv"), 
                                   outdir=paste0("tmp2",.x),
                                   shardby=c("Year","Month"),
                                   overwrite=T)) %>% 
    rbindlist.disk.frame(
      outdir = "inst/data/flights_all_shard.df", by_chunk_id = T)
  
  
  
  nrow(non_shard)
  nrow(sharded)
  
  library(disk.frame)
  sharded = disk.frame("inst/data/flights_all_shard.df")
  a = sharded[,.N, UniqueCarrier, keep="UniqueCarrier"]
  
  a = a[,sum(N), UniqueCarrier]
  
  nosharded = disk.frame("inst/data/flights_all_no_shard.df/")
  a1 = nosharded[,.N, UniqueCarrier, keep="UniqueCarrier"]
  a1 = a1[,sum(N), UniqueCarrier]
  
  setkey(a, UniqueCarrier)
  setkey(a1, UniqueCarrier)
  
  identical(a, a1)
  
