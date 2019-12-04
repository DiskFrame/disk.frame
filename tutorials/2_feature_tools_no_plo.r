library(disk.frame)
library(data.table)
library(drake)
library(DataExplorer)
# this willl set disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)

# rechunk(df, 32)
plan <- drake::drake_plan(
  df1 = {
    df1 = disk.frame(file_in("d:/data/ft.df"))
    if(nchunks(df1) != 32) {
      rechunk(df1, 32, shardby= "sk_id_curr")
    }
    df1
    },
  bads = df1 %>% 
    filter(target == 1) %>%
    collect(parallel=FALSE),
  sfrac = nrow(bads)/(nrow(df1) - nrow(bads)),
  goods = 
    df1 %>% 
      filter(target == 0) %>% 
      sample_frac(size = sfrac) %>% 
      collect(parallel = FALSE),
  gd = target(
    rbindlist(list(bads, goods)),
    format = "fst"
  ),
  arrow::write_parquet(gd, file_out("gd.parquet")),
  data.table::fwrite(gd, file_out("gd.csv")),
  gd_sample = sample_frac(gd, 0.25),
  data.table::fwrite(gd_sample, file_out("gd.sample.csv"))
)

make(plan)

loadd(gd)
# 
# profile_missing(gd) ->pmgd
# 
# plot_histogram(pmgd)
# 
# 
# intro_plot = DataExplorer::create_report(
#   gd,
#   configure_cache()
# )
#   
# introduce(gd)
