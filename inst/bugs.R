# TODO add these as test cases

library(disk.frame)
setup_disk.frame()

mtcars_df = as.disk.frame(mtcars, outdir = "mt_shard_by_cyl", shardby = "cyl", overwrite = TRUE)

mtcars_df %>% 
  collect_list

mtcars_df_2 = write_disk.frame(mtcars_df, outdir = "mt_shard_by_gear", shardby = "gear", overwrite = TRUE)

aa = mtcars_df %>% 
  hard_group_by("gear", nchunks = 3)

aa %>% 
  collect_list


aa = shard(mtcars_df, outdir = "mt_shard_by_gear", shardby = "gear", overwrite = T)


list.files("mt_shard_by_gear", full.names = TRUE) %>% 
  map(fst::read_fst)


