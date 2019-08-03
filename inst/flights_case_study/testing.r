library(disk.frame)
library(furrr)
library(magrittr)
plan(multiprocess)

# fs::dir_delete("tmp22007")
# fs::dir_delete("tmp22008")
# fs::dir_delete("tmp32007")
# fs::dir_delete("tmp32008")
# fs::dir_delete("tmp2007")
# fs::dir_delete("tmp2008")

pt = proc.time()
full = 1987:2008 %>% 
  furrr::future_map(~ csv_to_disk.frame(paste0("inst/flights_case_study/data/",.x,".csv"), 
                                 outdir=paste0("tmp3",.x),
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(
    outdir = "inst/data/flights_all_no_shard.df", by_chunk_id = T)
print(timetaken(pt))

cnames = names(full)

pt = proc.time()
non_shard = 1987:2008 %>% 
  furrr::future_map(~ csv_to_disk.frame(paste0("inst/flights_case_study/data/",.x,".csv"), 
                                 outdir=paste0("tmp",.x),
                                 in_chunk_size = 3e6,
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(
    outdir = "inst/data/flights_all_no_shard.df", by_chunk_id = T)
print(timetaken(pt))
  
pt = proc.time()
sharded = 1987:2008 %>% 
  purrr::map(~ csv_to_disk.frame(paste0("inst/flights_case_study/data/",.x,".csv"), 
                                 outdir=paste0("tmp2",.x),
                                 shardby=c("Year","Month"),
                                 col.names = cnames,
                                 in_chunk_size = 3e6,
                                 overwrite=T)) %>% 
  rbindlist.disk.frame(
    outdir = "inst/data/flights_all_shard.df", by_chunk_id = T)
print(timetaken(pt))

nrow(full)
nrow(non_shard)
nrow(sharded)

library(disk.frame)
# sharded = disk.frame("inst/data/flights_all_shard.df")
a = sharded[,.N, UniqueCarrier, keep="UniqueCarrier"]

a = a[,sum(N), UniqueCarrier]

# non_shard = disk.frame("inst/data/flights_all_no_shard.df/")
a1 = non_shard[,.N, UniqueCarrier, keep="UniqueCarrier"]
a1 = a1[,sum(N), UniqueCarrier]

setkey(a, UniqueCarrier)
setkey(a1, UniqueCarrier)

identical(a, a1)

a2 = merge(a,a1, by="UniqueCarrier")

a2[,all(V1.x == V1.y)]
